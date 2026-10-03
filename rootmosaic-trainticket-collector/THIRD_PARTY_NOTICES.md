# Third-party notices

`deployment/trainticket.yaml` is derived from the TrainTicket deployment
manifests in the source workspace's `workspace/trainticket/k8s/train-ticket-jaeger/`.
The corresponding upstream project is TrainTicket, distributed in that workspace
under Apache License 2.0. The complete license is included in
`third_party/TrainTicket-LICENSE`.

This distribution selects the 21 observed components plus Jaeger and uses
`imagePullPolicy: IfNotPresent`. Source file hashes and extraction notes are in
`SOURCE_PROVENANCE.json`. The upstream commit identifier was not established
from the available source snapshot.

External container images (TrainTicket/codewisdom, MongoDB, Jaeger, Prometheus,
and kube-state-metrics), Kubernetes, and Chaos Mesh are runtime dependencies.
Their source code and binaries are not bundled here. Their respective licenses
continue to apply. No license for the original RootMosaic collection scripts is
implied by the TrainTicket license.
