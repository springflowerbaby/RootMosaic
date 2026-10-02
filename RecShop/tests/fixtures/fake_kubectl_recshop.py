"""Stateful offline kubectl fixture. Never invokes kubectl or contacts a network."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import sys


def main():
    p = Path(os.environ["FAKE_KUBE_STATE"])
    state = json.loads(p.read_text()) if p.exists() else {"objects": {}, "assets": {}, "stage": {}, "calls": [], "seq": 0}
    args = sys.argv[1:]
    context, ns = None, None
    while args and args[0].startswith("--"):
        key = args.pop(0)
        if key == "--context":
            context = args.pop(0)
        elif key == "--namespace":
            ns = args.pop(0)
        elif not key.startswith("--request-timeout="):
            raise ValueError("unsupported global option")
    action = args.pop(0)
    state["calls"].append({"action": action, "context": context, "namespace": ns,
                           "args": args if action not in {"exec", "patch"} else args[:2]})
    raw = sys.stdin.buffer.read() if action in {"create", "apply", "delete"} else b""
    def key(kind, name):
        return kind.lower() + "/" + name
    def persist():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state), encoding="utf-8")
    def fail(message="fake rejection", echo=False):
        persist()
        sys.stderr.write(message)
        if echo:
            sys.stderr.buffer.write(raw)
        return 1
    result = None
    if action == "get" and args[0] == "--raw":
        url = args[1]
        if "/services/catalog:5005/proxy/api/items/RECSHOP_DEMO_001" in url:
            result = {"success": True, "item": {"item_id": "RECSHOP_DEMO_001"}}
        elif url.endswith("/health"):
            name = url.split("/services/")[1].split(":")[0]
            if name == "sasrec":
                result = {"status": "healthy", "model_loaded": os.environ.get("FAKE_BAD_HEALTH") != name}
            elif name == "rec-agent":
                result = {"recommendation_system": "healthy", "sasrec_service": {"status": "healthy", "model_loaded": True}}
                if os.environ.get("FAKE_BAD_HEALTH") == name:
                    result["sasrec_service"]["status"] = "unavailable"
            elif name == "shop-web":
                result = {"status": "ok", "service": "shop_web"}
            elif name == "backend":
                result = {"status": "healthy", "database": "healthy", "sasrec_api": "healthy"}
            else:
                result = {"status": "healthy", "service": name.replace("-", "_") + "_service", "database": "healthy"}
            if os.environ.get("FAKE_BAD_HEALTH") == name and name not in {"sasrec", "rec-agent"}:
                result["status"] = "degraded"
        else:
            return fail("unapproved request")
    elif action == "get" and args[:2] == ["deployments", "-o"]:
        result = {"items": [d for d in state["objects"].values() if d["kind"] == "Deployment"]}
    elif action == "get":
        result = state["objects"].get(key(args[0], args[1]))
    elif action in {"create", "apply"}:
        d = json.loads(raw)
        kind, name = d["kind"], d["metadata"]["name"]
        if os.environ.get("FAKE_LEAK_SECRET_ERROR") and kind == "Secret":
            return fail("fake server echoed sensitive request: ", echo=True)
        k = key(kind, name)
        if action == "create" and k in state["objects"]:
            return fail("already exists")
        old = state["objects"].get(k)
        state["seq"] += 1
        d["metadata"].update(uid=old["metadata"]["uid"] if old else "uid-" + str(state["seq"]),
                             resourceVersion=str(state["seq"]), generation=(old or {}).get("metadata", {}).get("generation", 0) + 1)
        if kind == "PersistentVolumeClaim":
            d["status"] = {"phase": "Pending"}
        elif kind in {"Deployment", "Pod"}:
            pod = d["spec"] if kind == "Pod" else d["spec"]["template"]["spec"]
            for v in pod.get("volumes", []):
                if "persistentVolumeClaim" in v:
                    claim = key("PersistentVolumeClaim", v["persistentVolumeClaim"]["claimName"])
                    state["objects"][claim]["status"] = {"phase": "Bound"}
            if kind == "Pod":
                d["spec"].setdefault("nodeName", "fake-node")
                d["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
                if os.environ.get("FAKE_HELPER_TERMINAL"):
                    d["status"] = {"phase": "Succeeded", "conditions": []}
            else:
                d["status"] = {"observedGeneration": d["metadata"]["generation"], "readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1}
        state["objects"][k] = d
        result = {"created": name}
    elif action == "rollout":
        name = args[1].split("/", 1)[1]
        d = state["objects"].get(key("Deployment", name))
        if not d or d["spec"].get("replicas") != 1:
            return fail("not ready")
        result = {"ready": name}
    elif action == "wait":
        resource = next(a for a in args if a.startswith("pod/"))
        present = key("Pod", resource.split("/")[1]) in state["objects"]
        if "--for=delete" in args and present:
            return fail("not deleted")
        if "--for=condition=Ready" in args and not present:
            return fail("not ready")
        if "--for=condition=Ready" in args and state["objects"][key("Pod", resource.split("/")[1])]["status"]["phase"] != "Running":
            return fail("terminal pod")
        result = {"waited": True}
    elif action == "exec":
        manifest = json.loads(base64.b64decode(args[-1]))
        mode = args[-2]
        missing = []
        for entry in manifest:
            name = entry["name"]
            if name in state["assets"]:
                if state["assets"][name] != entry:
                    return fail("remote mismatch")
            elif mode == "finalize":
                if state["stage"].get(name) != entry:
                    return fail("staged mismatch")
                state["assets"][name] = state["stage"].pop(name)
            else:
                missing.append(name)
        result = {"missing": missing}
    elif action == "cp":
        # Assert Windows-safe use: a local basename under cwd, never C:/ as cp source.
        name = args[0]
        if Path(name).name != name or ":" in name:
            return fail("source must be basename")
        content = Path(name).read_bytes()
        state["stage"][name] = {"name": name, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
        result = {"copied": name}
    elif action == "patch":
        d = state["objects"][key(args[0], args[1])]
        if os.environ.get("FAKE_REPLACE_BEFORE_PATCH") == args[1]:
            d["metadata"]["uid"] = "foreign-raced-uid"
            d["metadata"]["labels"]["recshop.dev/owner"] = "foreign-raced-owner"
        operations = json.loads(args[args.index("--patch") + 1])
        for op in operations:
            if op["op"] == "test":
                field = op["path"].split("/")[-1]
                if d["metadata"][field] != op["value"]:
                    return fail("precondition failed")
            elif op["path"] == "/spec/replicas" and op["value"] == 1:
                d["spec"]["replicas"] = 1
            elif op["path"] == "/spec":
                d["spec"] = op["value"]
            else:
                return fail("unsupported patch")
        result = {"patched": True}
    elif action == "delete" and args[0] == "--raw":
        url = args[1]
        if not url.endswith("/pods/recshop-assets-loader"):
            return fail("only loader pod deletion allowed")
        k = key("Pod", "recshop-assets-loader")
        d = state["objects"][k]
        if json.loads(raw)["preconditions"]["uid"] != d["metadata"]["uid"]:
            return fail("delete UID differs")
        del state["objects"][k]
        result = {"deleted": True}
    else:
        return fail("unsupported command")
    persist()
    if result is not None:
        print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
