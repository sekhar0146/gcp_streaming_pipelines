terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"  # Updated to match your environment
    }
  }
}

provider "google" {
  project = "gcp-learnings-498010"
  region  = "us-central1"
}