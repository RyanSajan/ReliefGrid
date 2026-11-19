# Acceptance checks and demonstration

Run these checks against an isolated deployment with test accounts and test data.

## Normal workflow

1. Provision an admin with the deployment instructions in the README.
2. Run `python scripts/smoke.py`. Set `SMOKE_ADMIN_USER` and `SMOKE_ADMIN_PASSWORD` for the manual match and request-status checks.
3. To show RabbitMQ delivery, inspect `docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers`. The worker consumes from `match_requests`.
4. Create requests until SHA-256 of `request:<id>` modulo 3 yields all three shard indices. Query `/internal/data?prefix=request:` on each shard through the Docker network or with Kubernetes port forwarding. Confirm each key is on its expected primary and replica.

## Docker failure and recovery

1. Choose a request hosted by shard 0. In the admin dashboard, confirm both copies are healthy.
2. Run `docker compose stop shard-0-primary`. Wait for its heartbeat to expire and check that the admin dashboard reports five healthy storage nodes. Retrieve the existing request, and create another request that hashes to shard 0.
3. Run `docker compose start shard-0-primary`. Confirm all six nodes report healthy. Query the restarted primary directly and confirm the record written during the outage appears after reconciliation.
4. Stop RabbitMQ, submit a request and register its matching resource, then start RabbitMQ. Poll the request and confirm pending work is requeued and matched.
5. Stop the coordinator and request data with a valid account. Check for HTTP 503, then restart the coordinator and verify that heartbeats restore storage access.

## Kubernetes failure and scaling

1. Run the README deployment steps and check all Deployments have ready pods and PVCs are bound.
2. Delete the API pod using `kubectl -n emergency delete pod -l app=request-service`. Watch the replacement become Ready and confirm that an existing account can sign in and retrieve its request. Restart port forwarding after the pod changes.
3. Run `kubectl -n emergency scale deployment/matcher --replicas=3` and wait for `kubectl -n emergency rollout status deployment/matcher`. Verify the replica count and rerun the smoke test while all three workers are running.
4. Repeat the storage check by scaling a storage Deployment to zero and back to one. A plain pod deletion may be too quick for the heartbeat to expire.

