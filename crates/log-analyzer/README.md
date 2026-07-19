# rustfs-log-analyzer

Offline log fault-analysis core for RustFS, powering the `rustfs diagnose`
subcommand: parses customer-provided logs (JSON lines, container-wrapped
output, panic blocks, archives), matches a built-in failure-rule library,
and produces severity-ranked diagnosis reports.

Plan and design contract: rustfs/backlog#1281.
