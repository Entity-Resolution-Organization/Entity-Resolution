"""
Entity Resolution — Push Model to GCP Artifact Registry

Packages the trained model as a Docker image and pushes it to GCP
Artifact Registry for versioned deployment.

What it does:
    1. Verifies trained model exists locally
    2. Checks quality gate passed (reads test_metrics.json)
    3. Determines next version from Artifact Registry
    4. Builds a minimal serving Docker image containing the model
    5. Pushes to Artifact Registry
    6. Logs deployment metadata to MLflow
    7. Saves rollback info (previous version) to registry_log.json
"""

import json
import os
import subprocess
import sys
from datetime import datetime

import mlflow
import yaml

DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_cmd(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return result."""
    print(f"[CMD] {cmd}")
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True
    )
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")
    return result


class RegistryPusher:
    """Builds and pushes model Docker image to GCP Artifact Registry."""

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        # Paths
        base_dir         = config["output"]["base_dir"]
        model_dir        = f"{base_dir}/{entity_type}"
        self.model_dir        = model_dir
        self.final_model_dir  = f"{model_dir}/final_model"
        self.results_dir      = f"{model_dir}/results"

        # GCP config
        gcp          = config["gcp"]
        ar           = gcp["artifact_registry"]
        self.project  = gcp["project_id"]
        self.region   = ar["location"]
        self.repo     = ar["repository"]
        image_name    = ar["image_name"].format(entity_type=entity_type)
        self.image_base = (
            f"{self.region}-docker.pkg.dev/{self.project}/{self.repo}/{image_name}"
        )

    # ------------------------------------------------------------------
    # Pre-flight checks
    # ------------------------------------------------------------------

    def check_model_exists(self):
        """Verify trained model is present before pushing."""
        if not os.path.isdir(self.final_model_dir):
            raise FileNotFoundError(
                f"Model not found at {self.final_model_dir}. "
                "Run train.py first."
            )
        print(f"[Push] Model found at {self.final_model_dir}")

    def check_quality_gate(self) -> dict:
        """Read test_metrics.json and verify quality gate passed."""
        metrics_path = os.path.join(self.results_dir, "test_metrics.json")
        if not os.path.exists(metrics_path):
            raise FileNotFoundError(
                f"test_metrics.json not found. Run evaluate.py first."
            )

        with open(metrics_path) as f:
            metrics = json.load(f)

        thresholds = self.config["validation"]
        checks = {
            "f1":        metrics.get("test_f1", 0)        >= thresholds["min_f1"],
            "precision": metrics.get("test_precision", 0) >= thresholds["min_precision"],
            "recall":    metrics.get("test_recall", 0)    >= thresholds["min_recall"],
            "auc":       metrics.get("test_auc", 0)       >= thresholds["min_auc"],
        }

        print("[Push] Quality gate check:")
        for metric, passed in checks.items():
            status = "PASS" if passed else "FAIL"
            print(f"  {metric}: {status}")

        if not all(checks.values()):
            raise RuntimeError(
                "Quality gate FAILED — model does not meet thresholds. "
                "Will not push to registry."
            )

        print("[Push] Quality gate PASSED")
        return metrics

    # ------------------------------------------------------------------
    # Version management
    # ------------------------------------------------------------------

    def get_next_version(self) -> tuple[str, str | None]:
        """
        Query Artifact Registry for existing tags and return next version.
        Returns (new_version_tag, previous_version_tag).
        """
        result = run_cmd(
            f"gcloud artifacts docker images list "
            f"{self.image_base} "
            f"--include-tags --format=json "
            f"--project={self.project}",
            check=False,
        )

        previous = None
        if result.returncode == 0 and result.stdout.strip():
            try:
                images = json.loads(result.stdout)
                tags   = []
                for img in images:
                    for tag in img.get("tags", "").split(","):
                        tag = tag.strip()
                        if tag.startswith("v") and tag[1:].isdigit():
                            tags.append(int(tag[1:]))
                if tags:
                    latest_num = max(tags)
                    previous   = f"v{latest_num}"
                    new_num    = latest_num + 1
                else:
                    new_num = 1
            except Exception:
                new_num = 1
        else:
            new_num = 1

        new_version = f"v{new_num}"
        print(f"[Push] Previous version: {previous or 'none'}")
        print(f"[Push] New version:      {new_version}")
        return new_version, previous

    # ------------------------------------------------------------------
    # Docker build + push
    # ------------------------------------------------------------------

    def write_serving_dockerfile(self, tmp_dir: str):
        """Write a minimal serving Dockerfile for the model."""
        ar           = self.config["gcp"]["artifact_registry"]
        base_image   = ar["serving_base_image"]
        packages     = " \\\n    ".join(ar["serving_packages"])

        dockerfile = f"""FROM {base_image}

WORKDIR /app

RUN pip install --no-cache-dir \\
    {packages}

COPY final_model/ ./model/

ENV MODEL_PATH=/app/model
ENV ENTITY_TYPE={self.entity_type}

CMD ["python", "-c", "print('Model ready at', __import__(chr(111)+chr(115)).environ['MODEL_PATH'])"]
"""
        path = os.path.join(tmp_dir, "Dockerfile.serve")
        with open(path, "w") as f:
            f.write(dockerfile)
        return path

    def build_and_push(self, version: str) -> str:
        """Build serving image and push to Artifact Registry."""
        image_uri    = f"{self.image_base}:{version}"
        image_latest = f"{self.image_base}:latest"

        # Configure Docker auth
        run_cmd(
            f"gcloud auth configure-docker {self.region}-docker.pkg.dev --quiet"
        )

        # Write temp Dockerfile — base dir from config
        tmp_base = self.config["gcp"]["artifact_registry"].get("tmp_build_dir", "/tmp/er_push")
        tmp_dir  = f"{tmp_base}_{self.entity_type}"
        os.makedirs(tmp_dir, exist_ok=True)
        # Copy model files into build context
        import shutil
        model_dest = os.path.join(tmp_dir, "final_model")
        if os.path.exists(model_dest):
            shutil.rmtree(model_dest)
        shutil.copytree(
            os.path.abspath(self.final_model_dir),
            model_dest
        )

        dockerfile_path = self.write_serving_dockerfile(tmp_dir)

        # Build
        run_cmd(
            f"docker build -f {dockerfile_path} -t {image_uri} -t {image_latest} {tmp_dir}"
        )

        # Push both tags
        run_cmd(f"docker push {image_uri}")
        run_cmd(f"docker push {image_latest}")

        print(f"[Push] Image pushed: {image_uri}")
        return image_uri

    # ------------------------------------------------------------------
    # Rollback info
    # ------------------------------------------------------------------

    def save_registry_log(
        self,
        version: str,
        previous: str | None,
        image_uri: str,
        metrics: dict,
    ):
        """Save deployment + rollback info to registry_log.json."""
        log = {
            "entity_type":    self.entity_type,
            "version":        version,
            "previous_version": previous,
            "image_uri":      image_uri,
            "rollback_uri":   f"{self.image_base}:{previous}" if previous else None,
            "pushed_at":      datetime.utcnow().isoformat(),
            "metrics":        metrics,
            "rollback_cmd": (
                f"docker pull {self.image_base}:{previous} && "
                f"docker tag {self.image_base}:{previous} {self.image_base}:latest"
            ) if previous else "no previous version",
        }

        log_path = os.path.join(self.results_dir, "registry_log.json")
        with open(log_path, "w") as f:
            json.dump(log, f, indent=2)
        print(f"[Push] Registry log → {log_path}")
        return log_path

    # ------------------------------------------------------------------
    # Main push()
    # ------------------------------------------------------------------

    def push(self):
        """Full push pipeline."""
        print(f"\n[Push] Starting push for entity_type={self.entity_type}")

        # 1. Pre-flight
        self.check_model_exists()
        metrics = self.check_quality_gate()

        # 2. Version
        version, previous = self.get_next_version()

        # 3. Build + push
        image_uri = self.build_and_push(version)

        # 4. Save rollback log
        log_path = self.save_registry_log(version, previous, image_uri, metrics)

        # 5. Log to MLflow
        mlflow.set_tracking_uri(self.config["mlflow"]["tracking_uri"])
        mlflow.set_experiment(self.config["mlflow"]["experiment_name"])

        with mlflow.start_run(
            run_name=f"push_registry_{self.entity_type}_{version}",
            tags={
                "entity_type": self.entity_type,
                "stage":       "registry_push",
                "version":     version,
            },
        ):
            mlflow.log_artifact(log_path, artifact_path="registry")
            mlflow.log_params({
                "image_uri":       image_uri,
                "version":         version,
                "previous_version": previous or "none",
            })
            mlflow.log_metrics(metrics)

        print(f"\n[Push] Done")
        print(f"  Image URI      : {image_uri}")
        print(f"  Version        : {version}")
        print(f"  Previous       : {previous or 'none'}")
        if previous:
            print(f"  Rollback cmd   : docker pull {self.image_base}:{previous}")

        return image_uri


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path  = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config       = load_config(config_path)
    entity_types = config["data"]["entity_types"]

    if not config["gcp"]["artifact_registry"]["enabled"]:
        print("[Push] Artifact Registry disabled in config — skipping")
        sys.exit(0)

    print("=" * 70)
    print("ENTITY RESOLUTION — PUSH TO ARTIFACT REGISTRY")
    print("=" * 70)

    for entity_type in entity_types:
        print(f"\n{'=' * 70}")
        print(f"Entity type: {entity_type.upper()}")
        print(f"{'=' * 70}")

        pusher = RegistryPusher(config=config, entity_type=entity_type)
        pusher.push()

    print("\n" + "=" * 70)
    print("PUSH COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()