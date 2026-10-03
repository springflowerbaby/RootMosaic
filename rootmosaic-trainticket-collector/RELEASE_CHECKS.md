# Package verification — 2026-10-03

Completed during preparation:

- All 6 offline release tests passed on Windows PowerShell 5.1.
- Both PowerShell scripts parsed without syntax errors.
- All bundled JSON files parsed successfully.
- TrainTicket YAML parsed as 47 resources; monitoring YAML as 12 resources.
- All 21 topology components have matching Deployment and Service resources.
- Scenario inventory: 55 unique slots, 275 planned runs, with valid target names
  and supported fault modes.
- Full-plan and selected-plan preview ran without contacting a cluster.
- Plan-only plus Initialize did not create output directories or alter files.
- Unknown slots were rejected, and Initialize refused to reset an existing index.
- The relocated Python root-signal auditor ran against one existing S01/r1 fault
  sample: one case and one root signal passed. This was a read-only check of an
  existing sample, not a new fault-injection experiment.
- A targeted text scan found no workstation absolute paths, private-key blocks,
  or common GitHub/Hugging Face/AWS token patterns in the release source files.
  This scan is not a comprehensive security audit.

Not validated during packaging: deployment against a fresh cluster, image pulls,
Chaos Mesh installation, live fault injection/recovery, or full data recollection.

The Hugging Face URL and a license for original collection code remain author
release metadata to complete. Third-party license text is included separately.
