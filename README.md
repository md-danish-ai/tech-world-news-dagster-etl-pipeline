# Tech World News Dagster ETL Pipeline

A Data Engineering pipeline orchestrated with **Dagster**, using **DuckDB** for analytical processing and **SQLite** for source data, containerized with Docker Compose.

---

## 🖥️ Services & Endpoints

| Service | URL | Description |
| :--- | :--- | :--- |
| **Dagster UI** | [http://localhost:6500](http://localhost:6500) | Pipeline orchestration, asset catalog & runs |
| **SQLite Web UI** | [http://localhost:8085](http://localhost:8085) | SQLite database browser |
| **DuckDB** | `localhost:8086` | Analytical warehouse container |

---

## 🚀 Quick Start

### 1. Start Services

```bash
docker compose up -d --build
```

Access the Dagster UI at **[http://localhost:6500](http://localhost:6500)**.

### 2. Common Commands

```bash
# Check container status
docker compose ps

# View live logs
docker compose logs -f

# Stop containers (preserves data)
docker compose down

# Full reset (removes containers and persistent volumes)
docker compose down -v
```

---

## 📁 Project Structure

```text
tech-world-news-dagster-etl-pipeline/
├── Dockerfile              # Dagster Docker image
├── docker-compose.yml      # Multi-container setup (Dagster, SQLite, DuckDB)
├── definitions.py          # Dagster definitions and assets
├── pyproject.toml          # Dagster module configuration
├── requirements.txt        # Python dependencies
├── dagster_home/           # Dagster instance settings (dagster.yaml)
└── data/                   # Shared database directory
```