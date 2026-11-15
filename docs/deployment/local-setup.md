# Windows 11 + WSL2 local setup

Keep the checkout under the Ubuntu filesystem, for example `~/finstream`, then open it with VS Code's WSL integration. Enable Docker Desktop WSL2 integration; do not run a large full stack on a 16 GB machine.

Install Python 3.11, Git, Docker Compose V2, and optionally Terraform. Create a venv, install `pip install -e '.[dev]'`, copy `.env.example` to `.env`, and set private local values. Run `make validate` first. Start profiles in phases: core for PostgreSQL; streaming for Kafka/Registry; api after DB health; spark only for stream work; monitoring when observing; airflow only for finite workflows.

Run `docker compose --env-file .env --profile <profile> config` before `up`. Record Docker memory use and reduce scope before adding services if the host swaps.

