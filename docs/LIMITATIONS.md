# Limitations and unsupported claims

One scheduler owns one journal; no multi-process lock, distributed scheduler, Kubernetes/Argo integration, containers, durable directory fsync on all platforms, leases, remote plugins, untrusted-code isolation, backoff timing, upstream OSS contribution, or production on-call history. Adapters must honor cancellation; a noncooperative adapter can prevent completion. Cancellation does not roll back completed effects. Direct Run callers must bind the workflow spec as the CLI does.

Tests exercise fixtures, not production traffic. Measurements include environment and exact commit in the receipt. A failure outcome is distinct from an unmeasured property. No years of experience, degrees, CVEs, production scale, mainnet ownership or independent audit are claimed.
