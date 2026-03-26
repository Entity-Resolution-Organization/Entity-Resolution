# Initial Setup

Terraform configuration to provision the core GCP infrastructure for the Entity Resolution project.

## Resources Created

- **Service Account:** `airflow-sa` (Entity Resolution Service Account) — Identity for Airflow, GCS, and BigQuery access (granted Storage Admin + BigQuery Admin). The authenticated user is automatically added as Owner.
- **VM:** `entity-resolution-vm` — e2-standard-4 instance running Ubuntu 22.04 LTS for pipeline workloads (Airflow, MLflow, Grafana).
- **GCS Bucket:** `entity-resolution-data-bucket` — Stores raw and processed datasets.
- **GCS Bucket:** `entity-resolution-dvc-bucket` — Remote storage backend for DVC-tracked artifacts.
- **Firewall Rules:** Opens ports for SSH (22), Airflow (8080), MLflow (5000), Grafana (3000), and Custom UI (8501).

## Step 1: Create the GCP Project

Create a GCP project manually via the [Google Cloud Console](https://console.cloud.google.com/).

- Project name: `Entity-Resolution`
- Project ID: `entity-resolution-487121`

Once created, enable the required APIs:

```bash
gcloud config set project entity-resolution-487121
gcloud services enable compute.googleapis.com storage.googleapis.com iam.googleapis.com bigquery.googleapis.com
```

## Step 2: Provision Cloud Resources

Run Terraform from your local machine to create all infrastructure.

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) installed
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) installed and authenticated:

```bash
gcloud auth application-default login
```

### Apply

```bash
cd Initial_Setup
terraform init
terraform plan     # review what will be created
terraform apply    # provision resources
```

### Verify

After applying, confirm the resources exist:

```bash
gcloud compute instances list
gcloud storage ls
gcloud iam service-accounts list
```

### Tear Down

```bash
terraform destroy
```