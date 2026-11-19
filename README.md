# ReliefGrid: Distributed Emergency Resource Matching System

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600.svg)](https://www.rabbitmq.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Minikube-326CE5.svg)](https://kubernetes.io/)

A fault-tolerant, distributed emergency response platform which coordinates emergency requests (water, food, medicine, transportation, shelter) with available volunteer resources using asynchronous task processing, sharded data storage with replication, and Kubernetes container orchestration.

---

## 🏛 Architecture Overview

```
                          ┌───────────────────────┐
                          │   Frontend Dashboard  │
                          │     (index.html)      │
                          └───────────┬───────────┘
                                      │ HTTP / REST
                                      ▼
                          ┌───────────────────────┐
                          │    Request Service    │
                          │   (FastAPI / Auth)    │
                          └───┬───────────────┬───┘
                              │               │
        Publish Match Task    │               │ Route Data Ops
                              ▼               ▼
┌───────────────────────────────┐           ┌──────────────────────────────┐
│     RabbitMQ Message Broker   │           │     Storage Coordinator      │
│     (Queue: match_requests)   │           │      (Hash Modulo 3)         │
└──────────────┬────────────────┘           └──┬────────────┬───────────┬──┘
               │                               │            │           │
   Consume Task│                 ┌─────────────┘            │           └─────────────┐
               ▼                 ▼                          ▼                         ▼
┌─────────────────────────┐ ┌─────────┐                ┌─────────┐               ┌─────────┐
│     Matcher Worker      │ │ Shard 0 │                │ Shard 1 │               │ Shard 2 │
│ (Location & Type Match) │ ├─────────┤                ├─────────┤               ├─────────┤
└─────────────────────────┘ │ Primary │                │ Primary │               │ Primary │
                            │ Replica │                │ Replica │               │ Replica │
                            └─────────┘                └─────────┘               └─────────┘
```

### Components

1. **Request Service (API & UI)**:
   - FastAPI server serving JSON APIs and the web portal (`frontend/index.html`).
   - Handles JWT authentication and role-based access for Citizens, Volunteers/NGOs, and Administrators.
   - Publishes match requests to RabbitMQ with automatic background requeuing if the broker is unreachable.

2. **RabbitMQ Message Broker**:
   - Decouples emergency request intake from backend resource allocation.
   - Queues matching requests durably (`match_requests`).

3. **Matcher Worker**:
   - Consumes pending match jobs from RabbitMQ asynchronously.
   - Matches requests to registered resources using emergency type and geographic city.
   - Coordinates transactional status updates via the distributed storage layer.

4. **Storage Coordinator**:
   - Determines shard placement deterministically via `SHA-256(key) % 3`.
   - Tracks node heartbeats (2-second intervals).
   - Routes reads/writes to healthy replicas and manages anti-entropy reconciliation after recovery.

5. **Storage Shards (SQLite Nodes)**:
   - 3 shards with 1 Primary and 1 Replica each (6 storage containers total).
   - Provide local persistent data isolation and high availability.

---

## ✨ Key Features

- **Asynchronous Task Processing**: Request ingestion returns immediately while the background matcher assigns resources asynchronously via RabbitMQ.
- **Broker Fault Tolerance & Requeuing**: If RabbitMQ goes down during request intake, the API stores requests locally and continuously detects and requeues pending jobs upon broker recovery.
- **Data Sharding & Active Replication**: Requests and resources are partitioned across 3 shards; any single node failure in a shard is seamlessly masked by its replica.
- **Dynamic Node Reconciliation**: Re-joining storage nodes automatically synchronize missing writes from active peers.
- **Web Portal & Management Dashboard**: Responsive, lightweight single-page application with distinct views for citizens, volunteers, and admins.

---

## 📂 Repository Structure

```
emergency-response-project/
├── app/
│   ├── common.py             # Shared storage utilities and data structures
│   ├── coordinator.py        # Shard hashing, heartbeat tracking, and routing
│   ├── create_admin.py       # CLI utility to provision administrative credentials
│   ├── matcher.py            # Asynchronous RabbitMQ match worker
│   ├── request_service.py    # Public REST API, auth logic, and broker retry daemon
│   └── storage.py            # Lightweight SQLite storage node implementation
├── docs/
│   ├── MEASUREMENT_PLAN.md   # Performance metrics and benchmarking guidelines
│   └── TEST_PLAN.md          # Acceptance tests, failover cases, and demo procedures
├── frontend/
│   └── index.html            # Web dashboard (Citizen, Volunteer, and Admin views)
├── k8s/
│   └── all.yaml              # Kubernetes manifests (Deployments, Services, PVCs)
├── scripts/
│   └── smoke.py              # End-to-end integration and smoke test suite
├── tests/
│   ├── locustfile.py         # Locust load test scenarios (concurrent users)
│   └── requirements.txt      # Load testing dependencies
├── .env.example              # Template environment configuration
├── docker-compose.yml        # Multi-container orchestration definition
├── Dockerfile                # Multi-service container image definition
├── README.md                 # Project documentation
└── requirements.txt          # Core Python application dependencies
```

---

## ⚙️ Prerequisites

- **Docker** (v24.0+) & **Docker Compose** (v2.20+)
- **Python** (v3.10+)
- **Kubernetes CLI (`kubectl`)** & **Minikube** (for Kubernetes deployment)

---

## 🚀 Local Setup (Docker Compose)

### 1. Configure Environment
Copy the sample environment file and generate a secure random authentication secret:
```bash
cp .env.example .env
```
Edit `.env` and replace `REPLACE_WITH_A_LONG_RANDOM_SECRET` with a secure random key:
```bash
# On Linux / macOS / Git Bash:
sed -i "s/REPLACE_WITH_A_LONG_RANDOM_SECRET/$(openssl rand -hex 32)/" .env

# Or manually paste a random string into .env
```

### 2. Start the Stack
Build and launch all 9 containers (RabbitMQ, Coordinator, Request Service, Matcher, and 6 Storage Shards):
```bash
docker compose up --build -d
```
Verify container statuses:
```bash
docker compose ps
```

### 3. Provision an Admin Account
```bash
docker compose exec request-service python -m app.create_admin
```
*(Enter your desired username and a password with at least 10 characters).*

### 4. Access the Platform
- **Web Dashboard**: [http://localhost:8000](http://localhost:8000)
- **Interactive OpenAPI Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **RabbitMQ Management Console**: [http://localhost:15672](http://localhost:15672) *(guest / guest)*

---

## ☸️ Kubernetes Deployment

Deploy the system locally using Minikube or Docker Desktop Kubernetes:

```bash
# 1. Build and load the Docker image into Minikube
docker build -t emergency-app:latest .
minikube image load emergency-app:latest

# 2. Create the dedicated namespace
kubectl create namespace emergency --dry-run=client -o yaml | kubectl apply -f -

# 3. Create the auth secret
kubectl -n emergency create secret generic reliefgrid-auth --from-literal=AUTH_SECRET="$(openssl rand -hex 32)"

# 4. Deploy all manifests (RabbitMQ, Coordinator, Storage Shards, Workers, API)
kubectl apply -f k8s/all.yaml

# 5. Check deployment rollout
kubectl -n emergency rollout status deployment/request-service
kubectl -n emergency rollout status deployment/matcher

# 6. Initialize the admin user
kubectl -n emergency exec -it deployment/request-service -- python -m app.create_admin

# 7. Port-forward the dashboard
kubectl -n emergency port-forward service/request-service 8000:8000
```

---

## 🧪 Testing & Validation

### 1. Smoke Tests
The smoke test script verifies end-to-end user creation, resource registration, request creation, automatic RabbitMQ matching, and administrative functions:

```bash
# Install test requirements
pip install -r requirements.txt

# Run smoke test suite
python scripts/smoke.py
```

To run with admin API validation:
```bash
export SMOKE_ADMIN_USER="admin"
export SMOKE_ADMIN_PASSWORD="YourAdminPassword"
python scripts/smoke.py
```

### 2. Load Testing (Locust)
Benchmark concurrent citizen requests and volunteer registrations under load:

```bash
cd tests
pip install -r requirements.txt
locust -f locustfile.py --host http://localhost:8000
```
Open [http://localhost:8089](http://localhost:8089) to configure the user count, spawn rate, and trigger benchmarks.


## 📡 REST API Summary

| Method | Endpoint | Description | Role / Auth |
|---|---|---|---|
| `POST` | `/auth/signup` | Register citizen or volunteer account | Public |
| `POST` | `/auth/login` | Authenticate and obtain JWT bearer token | Public |
| `POST` | `/requests` | Create an emergency assistance request | Citizen |
| `GET` | `/requests` | Retrieve current user's requests (or all for admin) | Authenticated |
| `GET` | `/requests/{id}` | Retrieve details of a specific request | Authenticated |
| `POST` | `/resources` | Register a resource/volunteer availability | Volunteer |
| `GET` | `/resources` | List registered resources | Authenticated |
| `PUT` | `/resources/{id}/status` | Toggle resource availability (AVAILABLE/UNAVAILABLE) | Volunteer / Admin |
| `PUT` | `/resources/{id}/verify` | Verify resource legitimacy | Admin |
| `POST` | `/match/{id}` | Manually trigger or retry matching for request | Admin |
| `GET` | `/system/health` | Node cluster health and heartbeat status | Admin |
| `GET` | `/system/metrics` | Matching progress metrics | Admin |
| `POST` | `/system/reconcile` | Force anti-entropy sync across shard replicas | Admin |

