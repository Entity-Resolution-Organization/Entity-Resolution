resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-vm"
  machine_type = "e2-standard-4"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = 50
    }
    auto_delete = true
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = google_service_account.airflow_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    startup-script = <<-SCRIPT
      #!/bin/bash
      set -e
      apt-get update -y
      apt-get install -y docker.io docker-compose git curl
      systemctl enable docker
      systemctl start docker
      usermod -aG docker ubuntu
      cd /home/ubuntu
      if [ ! -d "Entity-Resolution" ]; then
        git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
      fi
      cd Entity-Resolution
      bash setup.sh
    SCRIPT
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

resource "google_compute_firewall" "allow_inference_api" {
  name    = "allow-inference-api"
  network = "default"
  allow {
    protocol = "tcp"
    ports    = ["8000"]
  }
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["entity-resolution"]
}
