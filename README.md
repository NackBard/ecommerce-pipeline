# 🛒 Ecommerce Analytics Pipeline

A real-time data engineering pipeline that simulates an e-commerce event stream, processes it through **Apache Kafka** and **Apache Airflow**, stores results in **PostgreSQL**, and visualizes analytics in **Grafana** — all orchestrated with **Docker Compose**.

## 🏗️ Architecture

```
┌─────────────────┐     Kafka topic      ┌──────────────────────┐
│  Event Generator│ ──► ecommerce-events ──►   Apache Airflow   │
│  (.NET 9 / C#)  │    2–10 events/sec   │   DAG every 5 min    │
└─────────────────┘                      └──────────┬───────────┘
                                                     │
                                          batch insert (1000 events)
                                                     ▼
                                         ┌───────────────────────┐
                                         │     PostgreSQL 16     │
                                         │  raw.events (JSONB)   │
                                         │  mart.daily_revenue   │
                                         │  mart.conversion      │
                                         │  mart.cohort_ltv      │
                                         └──────────┬────────────┘
                                                     │
                                                     ▼
                                         ┌───────────────────────┐
                                         │       Grafana         │
                                         │  Daily Revenue        │
                                         │  Conversion Rate      │
                                         │  Order Per Days       │
                                         └───────────────────────┘
```

## ✨ Features

- 📡 **Event generator** — .NET 9 console app producing realistic e-commerce events to Kafka at 2–10 events/sec with weighted distribution (60% views, 25% orders, 10% payments, 5% registrations)
- 🔄 **Airflow DAG** — runs every 5 minutes: ingests a Kafka batch into `raw.events`, then aggregates into analytical marts
- 🗄️ **Two-layer schema** — `raw` layer for all incoming events (JSONB payload), `mart` layer with pre-aggregated tables for fast analytics queries
- 📊 **Grafana dashboard** — three panels: Daily Revenue, Conversion Rate, Best Order Days
- 🐳 **One-command startup** — full stack via `docker-compose up`

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Event generator | .NET 9, Confluent.Kafka |
| Message broker | Apache Kafka 7.5 + Zookeeper |
| Orchestration | Apache Airflow 2.8 |
| Processing | Python 3, confluent-kafka, psycopg2 |
| Database | PostgreSQL 16 |
| Visualization | Grafana 10.2 |
| Infrastructure | Docker, Docker Compose |

## 📊 Data Model

### `raw.events`
Append-only event log. All event types land here with a JSONB payload for schema flexibility.

```sql
event_type  VARCHAR(50)   -- item_viewed | order_created | order_paid | user_registered
user_id     UUID
payload     JSONB         -- varies by event type: {item_id, category} or {order_id, amount}
created_at  TIMESTAMPTZ
```

### Mart tables (aggregated by Airflow)

| Table | Description |
|---|---|
| `mart.daily_revenue` | Orders count, total revenue, avg order value per day |
| `mart.conversion` | Views vs orders ratio (conversion rate %) per day |
| `mart.cohort_ltv` | Revenue by user registration cohort month |

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose

### Run

```bash
git clone https://github.com/NackBard/ecommerce-pipeline.git
cd ecommerce-pipeline
docker-compose up -d
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Kafka | localhost:29092 |
| PostgreSQL | localhost:5432 |

### Start the event generator

```bash
cd generator
dotnet run
```

You'll see events flowing in the console:
```
Sent: item_viewed for user 3fa85f64-...
Sent: order_created for user 9b1c2a47-...
Sent: order_paid for user 3fa85f64-...
```

The Airflow DAG `ecommerce_pipeline` runs automatically every 5 minutes and populates the mart tables. Open Grafana to see the dashboards update in near real-time.

## ⚙️ Airflow DAG

The `ecommerce_pipeline` DAG has two sequential tasks:

```
consume_kafka  ──►  build_marts
```

**`consume_kafka`** — polls the `ecommerce-events` topic for 5 minutes, inserts events in batches of 1000 rows using `executemany`, commits the Kafka offset only after a successful DB commit. Rolls back and re-raises on any error.

**`build_marts`** — runs two `INSERT ... ON CONFLICT DO UPDATE` queries to upsert today's aggregated rows into `mart.daily_revenue` and `mart.conversion`.

## 🔮 Roadmap

- [x] **Grafana provisioning** — add `datasources/` and `dashboards/` YAML configs so the PostgreSQL datasource and Analytics dashboard load automatically on `docker-compose up` without any manual setup ✅
- [ ] **`mart.cohort_ltv` aggregation** — implement the Airflow task that populates the cohort LTV table (schema already exists in `init.sql`)
- [ ] **Dead-letter queue** — route malformed or unparseable events to a separate Kafka topic instead of silently dropping them
- [ ] **Kafka Schema Registry** — enforce Avro/JSON Schema on the `ecommerce-events` topic to prevent bad messages at the producer level
- [ ] **Airflow sensors** — replace the 5-minute polling interval with a Kafka sensor that triggers processing as soon as a threshold of messages accumulates
- [ ] **Data quality checks** — add Great Expectations or simple SQL assertion tasks to the DAG to catch anomalies (e.g. negative revenue, zero conversion days)
- [ ] **Partitioning** — partition `raw.events` by `created_at` month to keep query performance stable as data grows

## 📁 Project Structure

```
ecommerce-pipeline/
├── generator/
│   └── Program.cs                  # .NET 9 Kafka producer
├── processor/
│   └── dags/
│       └── ecommerce_pipeline.py   # Airflow DAG: ingest + aggregate
├── sql/
│   └── init.sql                    # PostgreSQL schema (raw + mart layers)
├── grafana/
│   └── dashboards/              
│   │   └── analytics.json          # Grafana dashboard definition
│   └── provisioning/
│       └── dashboards/             
│       │   └── providers.yml       # Grafana dashboards provider
│       └── datasources/
│           └── main.yml            # Grafana datasources
└── docker-compose.yml              # Full stack: Kafka, Airflow, Postgres, Grafana
```

---

> Built with ❤️ using .NET 9, Apache Kafka, Apache Airflow, PostgreSQL and Grafana
