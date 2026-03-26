# ---------- Provider ----------
# Configures the Google Cloud provider with project, region, and zone
provider "google" {
  project = "entity-resolution-487121"
  region  = "us-central1"
  zone    = "us-central1-a"
}

# ---------- Authenticated User ----------
# Fetches the email of the user running Terraform
data "google_client_openid_userinfo" "me" {}

# ---------- Service Account ----------
# Identity used by the VM for Airflow, GCS, and BigQuery access
resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-sa"
  display_name = "Entity Resolution Service Account"
  description  = "Service Account for Airflow Data Pipeline, GCS and BigQuery"
}

# ---------- IAM: Storage Admin ----------
# Grants the service account full access to GCS buckets
resource "google_project_iam_member" "sa_storage_admin" {
  project = "entity-resolution-487121"
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# ---------- IAM: BigQuery Admin ----------
# Grants the service account full access to BigQuery
resource "google_project_iam_member" "sa_bigquery_admin" {
  project = "entity-resolution-487121"
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# ---------- IAM: Service Account Owner ----------
# Grants the authenticated user Owner access on the service account
resource "google_service_account_iam_member" "sa_owner" {
  service_account_id = google_service_account.airflow_sa.name
  role               = "roles/owner"
  member             = "user:${data.google_client_openid_userinfo.me.email}"
}

# ---------- VM Instance ----------
# Compute Engine VM for running Airflow and pipeline workloads
resource "google_compute_instance" "entity_resolution_vm" {
  name         = "entity-resolution-vm"
  machine_type = "e2-standard-4"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = 50
    }
    auto_delete = true
  }

  network_interface {
    network = "default"
    access_config {
      # Assigns a public IP; remove this block for a private VM
    }
  }

  service_account {
    email  = google_service_account.airflow_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
  }

  shielded_instance_config {
    enable_secure_boot          = false
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  tags = ["entity-resolution"]
}

# ---------- Firewall: SSH ----------
# Allows SSH access to the VM
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}

# ---------- Firewall: Airflow ----------
# Allows access to the Airflow web UI (port 8080)
resource "google_compute_firewall" "allow_airflow" {
  name    = "allow-airflow"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}

# ---------- Firewall: MLflow ----------
# Allows access to the MLflow tracking UI (port 5000)
resource "google_compute_firewall" "allow_mlflow" {
  name    = "allow-mlflow"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["5000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}

# ---------- Firewall: Grafana ----------
# Allows access to the Grafana dashboard (port 3000)
resource "google_compute_firewall" "allow_grafana" {
  name    = "allow-grafana"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}

# ---------- Firewall: Custom UI ----------
# Allows access to the custom project UI (port 8501)
resource "google_compute_firewall" "allow_custom_ui" {
  name    = "allow-custom-ui"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8501"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}

# ---------- GCS Bucket: Data ----------
# Stores raw and processed datasets for the entity resolution pipeline
resource "google_storage_bucket" "data_bucket" {
  name          = "entity-resolution-data-bucket"
  location      = "us-central1"
  force_destroy = true
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

# ---------- GCS Bucket: DVC ----------
# Remote storage backend for DVC-tracked artifacts
resource "google_storage_bucket" "dvc_bucket" {
  name          = "entity-resolution-dvc-bucket"
  location      = "us-central1"
  force_destroy = true
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}