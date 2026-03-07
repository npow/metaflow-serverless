# metaflow-serverless

[![CI](https://github.com/npow/metaflow-serverless/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-serverless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-serverless)](https://pypi.org/project/metaflow-serverless/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Serverless Metaflow metadata service — free-tier Postgres, zero setup.

## The problem

Running Metaflow beyond your laptop means standing up a metadata service, a database, and object storage — typically on AWS, with always-on costs and hours of infra setup. For indie developers and small teams, that overhead kills momentum before the first experiment runs.

Existing alternatives either require a paid Outerbounds subscription or leave you managing servers that cost money even when idle.

## Quick start

```bash
pip install metaflow-serverless
mf-setup
python flow.py run
```

`mf-setup` provisions everything on free-tier serverless infrastructure and writes Metaflow config to `~/.metaflowconfig/config.json`. Authenticate first with `supabase login`.

## Install

```bash
pip install metaflow-serverless
```

Requires Python 3.10+. Metaflow must be installed separately:

```bash
pip install metaflow
```

## Usage

### Provision a new service

```bash
mf-setup
```

Walks you through choosing a provider stack, installs any needed CLIs, and provisions the database, storage, and compute layer. Writes credentials to `~/.metaflowconfig/config.json`.

### Supabase auth and config (important)

Authenticate the Supabase CLI before running setup:

```bash
supabase login
supabase projects list --output json
```

Then run:

```bash
mf-setup
```

The wizard writes Metaflow global config to:

```text
~/.metaflowconfig/config.json
```

### Supabase CLI v2 notes

- The setup flow is compatible with recent Supabase CLI versions where:
  - `supabase db url --project-ref ...` is removed.
  - `supabase storage create ... --project-ref ...` is removed.
- Migrations are applied directly via `asyncpg` and trigger a PostgREST schema reload.
- Edge function deploy uses a temporary `supabase/functions/<name>/` staging layout required by current CLI packaging.

### Supabase S3 credentials

`mf-setup` automatically provisions HMAC S3 credentials for your project and registers them in the database. No manual key management is needed. Credentials are written to `~/.metaflowconfig/config.json` and used for all artifact reads/writes.

### Run a flow

```python
# flow.py
from metaflow import FlowSpec, step

class MyFlow(FlowSpec):
    @step
    def start(self):
        self.data = [1, 2, 3]
        self.next(self.end)

    @step
    def end(self):
        print(self.data)

if __name__ == "__main__":
    MyFlow()
```

```bash
python flow.py run
```

Metadata and artifacts are recorded in your provisioned service automatically.

### Open the UI

```bash
mf-ui
```

Starts a local proxy on `localhost:8083` that downloads and serves the Metaflow UI, backed by your remote service. Supports flows list, run detail, DAG view, timeline view, task detail (attempt history, duration, status), and log streaming.

## How it works

`metaflow-serverless` replaces the Metaflow metadata service with a serverless stack — no persistent server required:

```
Metaflow client
  → Supabase Edge Function (URL router, ~40 lines TypeScript)
  → PostgREST (managed by Supabase)
  → PL/pgSQL stored procedures (heartbeat, tag mutation, artifact queries)
  → Postgres tables
```

All business logic lives in the database as stored procedures. The compute layer scales to zero and wakes in milliseconds. Artifacts are stored in S3-compatible object storage.

### Compute mode details

- With `compute=supabase`, `mf-setup` deploys a Supabase Edge Function named `metadata-router` (not the `netflixoss/metaflow-metadata-service` container image).
- The Edge Function handles HTTP routing and request shaping; metadata behavior lives in SQL procedures in Postgres.
- With `compute=cloud-run` or `compute=render`, `mf-setup` deploys the `netflixoss/metaflow-metadata-service` container image.

### Comparison: `metaflow-serverless` vs `metaflow-local-service`

| Dimension | `metaflow-serverless` | `metaflow-local-service` |
|---|---|---|
| Backend | Remote service (Supabase/Cloud Run/Render + Postgres) | Local daemon on `127.0.0.1` backed by `.metaflow/` |
| Cross-machine visibility | Yes | No (single machine by default) |
| Heartbeat tracking | Persisted in remote DB | In-memory daemon liveness only |
| Best use case | Shared/dev-prod-like metadata backend | Fast local iteration and CI |
| Remote Daytona/E2B observability | Recommended for end-to-end validation | Not equivalent to a shared remote backend |

## Provider stacks

| Stack | Accounts needed | Cold start | Storage |
|---|---|---|---|
| Supabase (default) | 1, email only, no CC | ~0ms | 1 GB free |
| Neon + Cloudflare R2 | 2, CC for R2 | ~1-4s | 10 GB free |
| CockroachDB + Backblaze B2 | 2, phone for B2 | ~1-4s | 10 GB free |

## Development

```bash
git clone https://github.com/npow/metaflow-serverless
cd metaflow-serverless
pip install -e ".[dev]"
pytest -v
```

## License

[Apache 2.0](LICENSE)
