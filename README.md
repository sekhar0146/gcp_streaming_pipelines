# Real-Time Data Streaming Pipeline (GCP & Apache Beam)

A production-ready, real-time data ingestion and enrichment pipeline built on Google Cloud Platform (GCP). The pipeline reads transaction streams from Pub/Sub, enriches the data with country metadata using Apache Beam side-inputs, and streams the final payload directly into BigQuery.

## Architecture Overview

The infrastructure is fully managed as code and processes data through the following stages:

1. **Ingest:** Simulated streaming data hits a **Google Cloud Pub/Sub** topic.
2. **Process & Enrich:** A **Google Cloud Dataflow** streaming job (Apache Beam) consumes the data, decodes the raw bytes, and performs a side-input dictionary lookup to add full country names and currency codes.
3. **Store:** The enriched dictionaries are streamed natively into **Google Cloud BigQuery** for immediate data analysis.
4. **Infrastructure as Code:** All infrastructure (Pub/Sub topics, BigQuery datasets/tables, and GCS dead-letter buckets) is provisioned dynamically via **Terraform**.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Framework:** Apache Beam
* **Runner:** Google Cloud Dataflow (Streaming Engine)
* **Storage / Analytics:** Google Cloud BigQuery
* **Messaging:** Google Cloud Pub/Sub
* **IaC:** Terraform

## 🚀 How to Deploy

### 1. Provision Infrastructure
Navigate to your Terraform folder and run:
terraform init
terraform apply

### 2. Deploy and Run the Streaming Pipeline
python streaming/s9_dataflow_to_bq.py `
  --runner=DataflowRunner `
  --project=gcp-learnings-498010 `
  --region=us-central1 `
  --staging_location=gs://gcp-learnings-498010-dataflow/staging `
  --temp_location=gs://gcp-learnings-498010-dataflow/temp `
  --job_name=streaming-enrichment-pipeline-v4 `
  --max_num_workers=1 `
  --num_workers=1 `
  --no_save_main_session

### Deployment Flag Reference:
--runner=DataflowRunner: Directs Apache Beam to run the pipeline as a managed cluster on GCP instead of locally.

--project & --region: Specifies your target GCP environment resources.

--staging_location & --temp_location: GCS paths where Dataflow stages the required binaries and worker packages.

--max_num_workers=1 & --num_workers=1: Allocates exactly one worker to keep compute costs highly predictable.

--no_save_main_session: Prevents worker start-up pickling crashes by skipping local main session serialization.

### 3. Generate Mock Traffic:
python publish_burst.py
