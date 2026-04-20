resource "google_bigquery_dataset" "entity_resolution" {
  dataset_id  = "entity_resolution"
  location    = "US"
  description = "Dataset for entity resolution prediction logs and clusters"
}
