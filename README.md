# metaflow-serverless

[![CI](https://github.com/npow/metaflow-serverless/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-serverless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-serverless)](https://pypi.org/project/metaflow-serverless/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Serverless Metaflow metadata service on free-tier Postgres — no always-on servers, no infra to manage.

## The problem

Running Metaflow beyond your laptop means standing up a metadata service, a database, and object storage — typically on AWS, with always-on costs and hours of infra setup. For indie developers and small teams, that overhead kills momentum before the first experiment runs.

Existing alternatives either require a paid Outerbounds subscription or leave you managing servers that cost money even when idle.

## Quick start

```bash
pip install metaflow-ephemeral-service
mf-setup
python flow.py run
```

`mf-setup` opens your browser for a one-time login, provisions everything on free-tier serverless infrastructure, and writes `~/.metaflowconfig`. No cloud account setup required beyond a free Supabase account.

## Install

```bash
pip install metaflow-ephemeral-service
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

Walks you through choosing a provider stack, installs any needed CLIs, authenticates via browser OAuth, and provisions the database, storage, and compute layer. Writes credentials to `~/.metaflowconfig`.

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

Starts a local proxy on `localhost:8083` that downloads and serves the Metaflow UI, backed by your remote service.

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
