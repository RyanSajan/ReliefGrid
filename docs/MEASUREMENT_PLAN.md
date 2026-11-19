# Engineering evaluation plan (targets, not verified results)

| Metric | Target | Measurement |
|---|---|---|
| API success availability | >=99.9% | Successful probes / total probes during a >=1 hour controlled test |
| GET request latency | p95 <200 ms | Locust at 100 users, measure p95 |
| Matching latency | p95 <2 seconds | Timestamp request creation and first MATCHED observation |
| Throughput | >=100 requests/s | Sustained Locust test with resource pool replenishment |
| Application pod recovery | <30 seconds | Kubernetes pod delete to ready replacement |
| Shard failure tolerance | 1 failed node per shard | Stop primary, confirm replica reads |
| Replica convergence | <60 seconds after restart | Restart primary and compare keys and versions after automatic reconciliation |
| Duplicate resource assignments | 0 in concurrency test | 100 simultaneous matching jobs for 1 resource |

**Limitations:** These figures are targets. The coordinator and broker are single services. Most requests continue through one available replica per shard. A write on only one copy can be lost if that copy fails before repair. Measure recovery and matching latency for the test environment used.
