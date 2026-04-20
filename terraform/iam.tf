import {
  to = google_service_account.airflow_sa
  id = "projects/${var.project_id}/serviceAccounts/airflow-sa@${var.project_id}.iam.gserviceaccount.com"
}

import {
  to = google_service_account.vertex_trainer_sa
  id = "projects/${var.project_id}/serviceAccounts/vertex-trainer@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-sa"
  display_name = "Entity Resolution Service Account"
  description  = "Service Account for Airflow Data Pipeline, GCS and BigQuery"
}

resource "google_service_account" "vertex_trainer_sa" {
  account_id   = "vertex-trainer"
  display_name = "Vertex Trainer Service Account"
  description  = "Service Account for Vertex AI training and Cloud Run inference"
}

resource "google_project_iam_member" "airflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "vertex_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.vertex_trainer_sa.email}"
}

resource "google_project_iam_member" "vertex_aiplatform_admin" {
  project = var.project_id
  role    = "roles/aiplatform.admin"
  member  = "serviceAccount:${google_service_account.vertex_trainer_sa.email}"
}

resource "google_project_iam_member" "vertex_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.vertex_trainer_sa.email}"
}

resource "google_project_iam_member" "vertex_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.vertex_trainer_sa.email}"
}

resource "google_project_iam_member" "vertex_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.admin"
  member  = "serviceAccount:${google_service_account.vertex_trainer_sa.email}"
}

resource "google_service_account_iam_member" "sa_owner" {
  service_account_id = google_service_account.airflow_sa.name
  role               = "roles/owner"
  member             = "user:${data.google_client_openid_userinfo.me.email}"
}
