# Football Data ETL Pipeline & Lakehouse

This project collects football match data from the web, cleans and organizes it in a multi-layer data lakehouse, and prepares it for analysis. It uses Docker for containerization, and Apache Spark with Delta Lake for processing.

## Core Idea & Architecture

The primary goal is to build a scalable and reliable data platform that transforms raw, nested JSON data into clean, structured, and aggregated tables suitable for business intelligence and data science.

### Layers

1.  **Bronze Layer**: Raw JSON files scraped from whoscored site landed in HDFS. Then, a Spark job reads from these files, applies a basic schema, and saves them as Bronze Delta tables. This one-time conversion helps improve performance for later processes.

2.  **Silver Layer**: Create a single source of truth. Spark job reads the data from Bronze Delta tables, performs operations like flattening, joining, cleaning data, and standardizing data types. The result will be a set of clean, atomic fact and dimension tables, ready for ad-hoc querying and foundational analytics.

3.  **Gold Layer**: Provide a refined, aggregated data for reporting and analytics. Spark jobs read from clean Silver tables and perform aggregations. After this process, Gold tables will be small, fast aggregated tables, answering business questions.

### Key Technologies

*   **Apache Spark:** The core distributed processing engine for all ETL transformations.
*   **Delta Lake:** The storage format that turns our HDFS data lake into a reliable **Data Lakehouse**. It provides ACID transactions, schema enforcement, and time travel capabilities.
*   **HDFS:** Distributed file system used as the data lake.
*   **Docker & Docker Compose:** Containerizes the entire Spark cluster and its dependencies, ensuring a consistent and portable development and execution environment.
*   **Python:** The primary language for orchestration, crawling, and Spark transformations.
*   **Selenium:** Used for the web crawling module to extract raw data.

---

## Project Structure

```text
football-data/
├── main.py                    # Main orchestrator to run the entire ETL pipeline (Bronze -> Silver -> Gold).
├── docker-compose.yml         # Defines the Spark cluster, HDFS, and other services.
├── .env.example               # Template for environment variables.
|
├── analysis/                  # Notebooks for ad-hoc analysis, debugging, and showcasing results.
│   ├── databricks_sandbox.ipynb # Notebook on databricks. Uploads file to Databricks to use this
│   └── sandbox.ipynb            # Local notebook for testing and analysing
|
├── configs/                   # All project configuration files.
│   ├── schemas.py               # For schemas enforce from raw JSON to Bronze Delta tables
│   ├── league_mapping.json      # Configuration for the crawler.
│   ├── event_type_mapping.json  # Build event type in str from int for easier analytics 
│   ├── position_mapping.json    # Get player position from formation + slot_id
│   ├── qualifier_mapping.json   # For analytics purpose
│   └── config.yaml              # Defines simple aggregations on cols and final Gold col order.
|
├── jobs/                      # Standalone, runnable scripts for manual or scheduled tasks.
│   ├── run_full_batch_crawl.py  # Initial, large-scale web crawl. This should take very long.
│   ├── generate_refresh_plan.py # Spark job to find data to be re-crawled.
│   └── execute_refresh_crawl.py # Selenium job to perform the refresh crawl.
|
├── process/                   # Core ETL logic for the Bronze and Silver layers.
│   ├── bronze_pipeline.py       # Orchestrates ingestion from raw JSON files to Bronze tables.
│   ├── silver_pipeline.py       # Orchestrates transformations from Bronze to Silver.
│   └── silver/                  # Modules with detailed business logic for Silver tables.
│       ├── players.py
│       ├── teams.py
│       └── ...
|
├── aggregations/              # ETL logic for the Gold layer.
│   ├── gold_pipeline.py         # Orchestrates aggregations from Silver to Gold.
│   └── gold/                    # Modules with detailed aggregation logic.
│       ├── player_match_stats.py
│       └── ...
|
├── scripts/                   # Utility scripts for one-off operational tasks.
│   └── upload_to_databricks.py  # Manual script to push Gold layer data to Databricks.
|
├── crawler/                   # A library of reusable helper functions for web crawling.
│   └── utils.py
|
└── utils/                     # A library of generic, reusable utilities for the entire project.
    ├── io_utils.py
    ├── spark_session.py
    └── logging_utils.py
```

## Setup & Instructions

### 1. Prerequisites

Before you begin, ensure you have the following installed on your system:

*   **Docker & Docker Compose:** For running the containerized Spark cluster.
*   **Python 3.10+:** For running the crawler and orchestration scripts.
*   **An accessible HDFS Cluster:** The pipeline is configured to read from and write to HDFS.
*   **Google Chrome Browser:** The Selenium crawler requires a full installation of the Google Chrome browser to function.
*   **VPN**: Preferably use a VPN to crawl data. 

### 2. Initial Setup

Follow these steps to configure the project on your local machine.

1.  **Clone the Repository**

    ```bash
    git clone github.com/imdottier/football_data
    cd football-data
    ```

2.  **Configure Environment Variables**

    Copy the template file to create your own local configuration, then edit it with your specific paths and credentials.
    ```bash
    cp .env.example .env
    nano .env
    ```

3.  **Install Python Dependencies**

    It is highly recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

4.  **Install Google Chrome**

    If you don't have it installed, download the official `.deb` package and install it via the command line.
    ```bash
    # (First, download google-chrome-stable_current_amd64.deb from the official site)
    sudo dpkg -i google-chrome-stable_current_amd64.deb
    sudo apt-get install -f # This command fixes any missing dependencies
    ```

5.  **Start the Services**

    Launch the Spark and other necessary containers from the project root.
    ```bash
    docker-compose up -d
    ```

### 3. Running the Pipeline

The main workflow consists of two major steps: an initial data crawl, followed by the Spark ETL processing.

**Perform the Initial Data Crawl**

This script scrapes all historical data as defined in `configs/league_mapping.json`. **This is a long-running process.**
You should turn on your VPN in this step.

```bash
python -m jobs.run_full_batch_crawl
```

**Run the Full ETL Pipeline (Bronze -> Silver -> Gold)**
This command submits the main orchestration script to your Spark cluster. It processes the raw data from the crawl through all layers. Install compatible delta-spark version here to work with Delta tables. 

```bash
docker-compose exec spark-master spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /app/main.py --layers all
```

### 4. Optional Next Steps

After the pipeline has run successfully, you can explore the final tables.

**Jupyter Notebook**: Open and run analysis/sandbox.ipynb for sample queries and visualizations.

**Upload to Databricks**: Push the final Gold tables to a Databricks workspace using the utility script.

```bash
python -m scripts.upload_to_databricks
```

**Refreshing Recent Data (Incremental Crawl)**

To update the data with recent matches without re-scraping everything, run the two-stage refresh process.

Generate a plan of what to crawl. This Spark job finds recent/unfinished matches and creates a plan.

```bash
docker-compose exec spark-master spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /app/jobs/generate_refresh_plan.py
```

Execute the crawl based on the plan. This Selenium job reads the plan and scrapes only the required matches.
You should turn on your VPN in this step.

```bash
python -m jobs.execute_refresh_crawl
```