output "vm_external_ip" {
  description = "External IP of the Airflow VM"
  value       = google_compute_instance.airflow_vm.network_interface[0].access_config[0].nat_ip
}

output "artifact_registry_url" {
  description = "Artifact Registry URL for pushing Docker images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ml_models.repository_id}"
}

output "main_bucket" {
  description = "Main GCS bucket name"
  value       = google_storage_bucket.main_bucket.name
}
