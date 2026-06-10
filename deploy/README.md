# Production deployment: multi-instance maestro-server on EC2 with Docker

Runs **3 `maestro-server` instances** behind an nginx load balancer on a single EC2 host, all
sharing **your external Postgres**. Clustering is coordinated through Postgres (flow-group
ownership + heartbeats) — **no Redis or AWS SNS/SQS is required**.

## Files
| File | Purpose |
|------|---------|
| `../Dockerfile` | Multi-stage build → Spring Boot fat jar on a JRE 21 image |
| `docker-compose.yml` | 3 app nodes (`maestro-1/2/3`) + `lb` (nginx) |
| `application-prod.yml` | `prod` Spring profile; mounted into each container at `/config/` |
| `nginx.conf` | Round-robin LB across the nodes on `:80` |
| `maestro.env.example` | DB endpoint + cluster sizing (copy to `maestro.env`) |

## Prerequisites on the EC2 box
- Docker + Docker Compose v2, Java not needed on the host (built inside the image).
- Network access from EC2 to your Postgres (security group allows EC2 → Postgres:5432).
- A Postgres database for Maestro (Flyway creates the schema on first boot).

## Deploy
```bash
cd deploy
cp maestro.env.example maestro.env
#   edit maestro.env: MAESTRO_DB_URL / USERNAME / PASSWORD
docker compose up -d --build

# verify
docker compose ps
curl http://localhost/healthz
curl http://localhost/actuator/health         # proxied to a node; expect {"status":"UP"}
docker compose logs -f maestro-1               # watch "[...] claimed a group [...]"
```

Each node logs which flow groups it claims. Across the 3 nodes the 9 groups (see
`MAESTRO_MAX_GROUP_NUM`) spread out; kill a node and the survivors re-claim its groups within ~1 min.

## Scaling
The number of nodes is fixed by the service list. To change it: add/remove `maestro-N` services,
list them in `nginx.conf`'s `upstream`, and set `MAESTRO_MAX_GROUP_NUM = 3 × node_count`
(keep `PER_NODE = 5`). Per the engine's design, prefer a **fresh full-cluster redeploy** with a
larger group count over adding nodes to a running cluster.

## Front it properly
- Put an **ALB** (TLS termination) in front of EC2:80, and lock the security group so only the ALB
  reaches the instance.
- The app does **no authentication** — mutating calls just trust a `user:` header. Enforce real
  auth at the ALB / an auth proxy before exposing this beyond a trusted network.

## Caveats
- **Single EC2 = single point of failure.** Multiple app *processes* survive one node crashing, but
  not the host dying. For real HA, run this compose (or the image) on ≥2 EC2 hosts behind the ALB,
  all pointed at the same Postgres.
- **Postgres is the critical shared state** — back it up and size it for the workflow load.
- Redis/SNS/SQS features (AWS step concurrency, time/signal triggers, event notifications) are
  intentionally off here. Enabling them requires the `aws` profile and real AWS messaging infra.
