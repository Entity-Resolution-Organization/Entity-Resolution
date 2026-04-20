resource "google_secret_manager_secret" "er_env_model" {
  secret_id = "er-env-model"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "er_env_monitoring" {
  secret_id = "er-env-monitoring"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "er_env_data" {
  secret_id = "er-env-data"

  replication {
    auto {}
  }
}
