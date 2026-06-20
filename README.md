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
```bash
terraform init
terraform apply
