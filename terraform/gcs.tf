resource "google_storage_bucket" "main_bucket" {
  name          = var.bucket_name
  location      = "US"
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = false

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "staging_bucket" {
  name          = var.staging_bucket_name
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_bucket" {
  name          = "entity-resolution-data-bucket"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "dvc_bucket" {
  name          = "entity-resolution-dvc-bucket"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}
