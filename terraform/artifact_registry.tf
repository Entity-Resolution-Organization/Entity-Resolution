import {
  to = google_artifact_registry_repository.ml_models
  id = "projects/${var.project_id}/locations/${var.region}/repositories/ml-models"
}

resource "google_artifact_registry_repository" "ml_models" {
  location      = var.region
  repository_id = "ml-models"
  format        = "DOCKER"
  description   = "Docker images for Entity Resolution ML pipelines"
}
