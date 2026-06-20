# 1. Define the Production-Grade Input Pub/Sub Topic
resource "google_pubsub_topic" "transactions_input" {
  name = "transactions-input"

  labels = {
    environment = "development"
    pipeline    = "transaction-processor"
  }
}

# 2. Define the Pull Subscription attached directly to that Topic
resource "google_pubsub_subscription" "transactions_input_sub" {
  name  = "transactions-input-sub"
  topic = google_pubsub_topic.transactions_input.name

  # Keep messages in the queue safely for 20 minutes if the pipeline stops
  message_retention_duration = "1200s"

  # Acknowledgment deadline: Time given to Beam to process an element before retry
  ack_deadline_seconds = 20
}

# 3. Define the Cloud Storage Bucket for DLQ Dead Letters
resource "google_storage_bucket" "dlq_bucket" {
  name          = "dlq_bucket-498010" # Must be globally unique across GCP
  location      = "US-CENTRAL1"
  storage_class = "STANDARD"

  # Enforce prevention of public visibility for data safety
  public_access_prevention = "enforced"

  # Automatically destroy objects inside if you run 'terraform destroy' later
  force_destroy = true

  # Automatically delete records after 30 days to optimize storage costs
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}
# ============================================================================
# NEW BIGQUERY RESOURCES
# ============================================================================

# 4. Define the BigQuery Dataset (Logical Folder)
resource "google_bigquery_dataset" "txns_dataset" {
  dataset_id                  = "txns_dataset"
  friendly_name               = "Transactions Data Warehouse"
  description                 = "Contains raw and enriched real-time transaction streams"
  location                    = "us-central1" # Must match Dataflow and Pub/Sub location
  delete_contents_on_destroy = false
}

# 5. Define the Production Enriched Table with Schema, Partitioning, and Clustering
resource "google_bigquery_table" "enriched_transactions" {
  dataset_id          = google_bigquery_dataset.txns_dataset.dataset_id
  table_id            = "enriched_transactions"
  deletion_protection = false # Set to true in real production to prevent accidental deletes

  # Time Partitioning: Isolates data into physical blocks based on arrival day.
  # This stops BigQuery from scanning your entire table history when querying recent logs.
  time_partitioning {
    type  = "DAY"
    field = "timestamp" # Maps directly to the pipeline's timestamp key
  }

  # Clustering: Colocates rows with the same country code together on disk.
  # Extremely fast for WHERE country = 'IN' style queries.
  clustering = ["country"]

  # The explicit database schema expected by the Apache Beam pipeline
  schema = <<EOF
[
  {
    "name": "txn_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique transaction identification string"
  },
  {
    "name": "country",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "ISO 2-letter country code"
  },
  {
    "name": "amount",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "Monetary value of transaction"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Event processing time execution"
  },
  {
    "name": "country_full_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Enriched full country name text from lookup step"
  },
  {
    "name": "currency",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Enriched transaction currency code"
  }
]
EOF
}