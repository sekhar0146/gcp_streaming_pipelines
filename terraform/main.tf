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