# Pokémon ETL and DAG Generator

<img src="assets/logo.jpeg" alt="Wild Airflow appeared!" width="50%">

---

This project is an **ETL pipeline** and **dynamic DAG generator** built with Apache Airflow to collect, transform, and load Pokémon data from multiple sources: Pokédex API, Pokémon TCG API, and CardMarket price history (via web crawling). It demonstrates a modular data engineering workflow with custom operators, multiple data ingestion strategies, and automated pipeline creation.

---

## Features

- **ETL Pipelines** for multiple Pokémon datasets:
  - Pokédex data
  - Pokémon TCG card information
  - CardMarket price history
- **Custom Airflow Operators** for API extraction, S3 storage, and Postgres loading
- **Dynamic DAG Factory** for programmatic DAG generation
- **Dockerized environment** with `docker-compose` for easy local setup
- **Postgres integration** with defined schemas and staging/structured layers

---

## Tech Stack

- **Apache Airflow** (orchestration)
- **Python** (custom operators and API integration)
- **Postgres** (data storage)
- **AWS S3** (staging layer)
- **Docker & Docker Compose** (containerization)
- **Metabase** (visualization)

---

## Data Sources

- [Pokédex API](https://pokeapi.co/) – Detailed Pokémon data (abilities, stats, types)  
- [Pokémon TCG API](https://pokemontcg.io/) – Card sets, prices, legality info  
- [CardMarket](https://www.cardmarket.com/en/Pokemon) – Web scraped for historical price data

---

## Data Architecture

- **Landing**: Temporary tables for extracting and loading data.
- **Currentraw**: First layer for raw data in any format.
- **Historyraw**: History of currentraw schema.
- **Structured**: Second layer for data flattening.
- **Trusted**: Third layer for normalization and data processing.

---

## Project Structure

```
project-etl-pokemon/
│
├── dags/                       # Airflow DAGs
│   ├── dag_pokedex_pokemon.py
│   ├── dag_poketcg_cards.py
│   ├── dag_cardmarket_history.py
│   └── custom/
│       ├── functions/          # API integration and webcrawler functions
│       └── operators/          # Custom Airflow operators
│
├── scripts/                    # SQL schemas and structured tables
│   ├── postgres_schemas.sql
│   └── postgres_structured.sql
│
├── docker-compose.yaml         # Local Airflow + Postgres environment
├── Dockerfile
├── requirements.txt
├── CHANGELOG.md
└── assets/
    ├── logo.jpeg
```

---

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- (Optional) AWS credentials if integrating with S3 Bucket

### Setup

```bash
# Clone this repository
git clone https://github.com/luizgnf/project-etl-pokemon.git
cd project-etl-pokemon

# Copy the sample environment file and fill in your secrets
cp .env.profile.example .env.profile

# Build and start the containers
make compose-build && make compose-up

# For other useful commands
make help
```

### Access Airflow UI

- Airflow Webserver: `http://localhost:8080`

### Running Pipelines

- Enable the DAGs for:
  - `dag_pokedex_pokemon`
  - `dag_poketcg_cards`
  - `dag_cardmarket_history`

### Access Metabase UI

- Metabase: `http://localhost:3000`

---

