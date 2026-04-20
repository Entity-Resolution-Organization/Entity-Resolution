resource "google_artifact_registry_repository" "ml_models" {
  location      = var.region
  repository_id = "ml-models"
  format        = "DOCKER"
  description   = "Docker images for Entity Resolution ML pipelines"
}
