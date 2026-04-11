"""
Entity Resolution — Push Model to GCP Artifact Registry

Packages the trained model as a Docker image and pushes it to GCP
Artifact Registry for versioned deployment.

What it does:
    1. Verifies trained model exists locally
    2. Checks quality gate passed (reads test_metrics.json)
    3. Determines next version from Artifact Registry
    4. Builds a serving Docker image containing:
           - LoRA adapter weights  (→ /app/model_weights/)
           - serve.py              (→ /app/scripts/serve.py)
           - training_config.yaml  (→ /app/config/)
    5. Pushes to Artifact Registry
    6. Logs deployment metadata to MLflow
    7. Saves rollback info (previous version) to registry_log.json
"""

import json
import os
import shutil
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
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")
    return result


class RegistryPusher:

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        base_dir             = config["output"]["base_dir"]
        model_dir            = f"{base_dir}/{entity_type}"
        self.model_dir       = model_dir
        self.final_model_dir = f"{model_dir}/final_model"
        self.results_dir     = f"{model_dir}/results"

        gcp             = config["gcp"]
        ar              = gcp["artifact_registry"]
        self.project    = gcp["project_id"]
        self.region     = ar["location"]
        self.repo       = ar["repository"]
        image_name      = ar["image_name"].format(entity_type=entity_type)
        self.image_base = (
            f"{self.region}-docker.pkg.dev/{self.project}/{self.repo}/{image_name}"
        )

    # ------------------------------------------------------------------
    # Pre-flight checks
    # ------------------------------------------------------------------

    def check_model_exists(self):
        if not os.path.isdir(self.final_model_dir):
            raise FileNotFoundError(
                f"Model not found at {self.final_model_dir}. Run train.py first."
            )
        print(f"[Push] Model found at {self.final_model_dir}")

    def check_quality_gate(self) -> dict:
        metrics_path = os.path.join(self.results_dir, "test_metrics.json")
        if not os.path.exists(metrics_path):
            raise FileNotFoundError(
                "test_metrics.json not found. Run evaluate.py first."
            )

        with open(metrics_path) as f:
            metrics = json.load(f)

        thresholds = self.config["validation"]
        checks = {
            "f1":        metrics.get("test_f1",        0) >= thresholds["min_f1"],
            "precision": metrics.get("test_precision", 0) >= thresholds["min_precision"],
            "recall":    metrics.get("test_recall",    0) >= thresholds["min_recall"],
            "auc":       metrics.get("test_auc",       0) >= thresholds["min_auc"],
        }

        print("[Push] Quality gate check:")
        for metric, passed in checks.items():
            print(f"  {metric}: {'PASS' if passed else 'FAIL'}")

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
        print(f"[Push] Previous version : {previous or 'none'}")
        print(f"[Push] New version      : {new_version}")
        return new_version, previous

    # ------------------------------------------------------------------
    # Docker build + push
    # ------------------------------------------------------------------

    def write_serving_dockerfile(self, tmp_dir: str) -> str:
        """
        Write serving Dockerfile that launches serve.py via uvicorn.

        Image layout:
            /app/model_weights/   ← LoRA adapter weights (baked in)
            /app/scripts/serve.py ← FastAPI server
            /app/config/          ← training_config.yaml
        """
        ar         = self.config["gcp"]["artifact_registry"]
        base_image = ar["serving_base_image"]
        # Base serving packages from config + FastAPI stack for serve.py
        packages   = ar["serving_packages"] + ["fastapi", "uvicorn[standard]", "pydantic"]
        packages_str = " \\\n    ".join(packages)

        dockerfile = f"""FROM {base_image}

WORKDIR /app

RUN pip install --no-cache-dir \\
    {packages_str}

RUN python -c "
from transformers import AutoModelForSequenceClassification, AutoTokenizer
AutoTokenizer.from_pretrained('microsoft/deberta-v3-base', cache_dir='/app/cache')
AutoModelForSequenceClassification.from_pretrained(
    'microsoft/deberta-v3-base', num_labels=2, cache_dir='/app/cache')
"
ENV TRANSFORMERS_CACHE=/app/cache

# LoRA adapter weights baked into image
COPY model_weights/ ./model_weights/

# Serving code and config
COPY scripts/serve.py ./scripts/serve.py
COPY config/          ./config/

ENV MODEL_DIR=/app/model_weights
ENV CONFIG_PATH=/app/config/training_config.yaml
ENV ENTITY_TYPE={self.entity_type}

EXPOSE 8080

# Vertex AI health-checks GET /health and routes POST /predict
CMD ["uvicorn", "scripts.serve:app", "--host", "0.0.0.0", "--port", "8080"]
"""
        path = os.path.join(tmp_dir, "Dockerfile.serve")
        with open(path, "w") as f:
            f.write(dockerfile)
        return path

    def _prepare_build_context(self, tmp_dir: str):
        """
        Assemble Docker build context in tmp_dir:
            tmp_dir/
                model_weights/   ← final_model_dir contents
                scripts/
                    serve.py
                config/
                    training_config.yaml
                Dockerfile.serve
        """
        # 1. Model weights
        model_dest = os.path.join(tmp_dir, "model_weights")
        if os.path.exists(model_dest):
            shutil.rmtree(model_dest)
        shutil.copytree(os.path.abspath(self.final_model_dir), model_dest)
        print(f"[Push] Copied model weights → {model_dest}")

        # 2. serve.py — resolve relative to this script's location
        scripts_dest = os.path.join(tmp_dir, "scripts")
        os.makedirs(scripts_dest, exist_ok=True)
        serve_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "serve.py")
        if not os.path.exists(serve_src):
            raise FileNotFoundError(
                f"serve.py not found at {serve_src}. "
                "Ensure serve.py is in the same directory as push_to_registry.py."
            )
        shutil.copy(serve_src, os.path.join(scripts_dest, "serve.py"))
        print(f"[Push] Copied serve.py → {scripts_dest}/")

        # 3. Config directory
        config_src  = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "config"
        )
        config_dest = os.path.join(tmp_dir, "config")
        if os.path.exists(config_dest):
            shutil.rmtree(config_dest)
        shutil.copytree(os.path.abspath(config_src), config_dest)
        print(f"[Push] Copied config → {config_dest}/")

    def build_and_push(self, version: str) -> str:
        image_uri    = f"{self.image_base}:{version}"
        image_latest = f"{self.image_base}:latest"

        # Configure Docker auth for Artifact Registry
        run_cmd(
            f"gcloud auth configure-docker {self.region}-docker.pkg.dev --quiet"
        )

        # Prepare build context
        tmp_base = self.config["gcp"]["artifact_registry"].get(
            "tmp_build_dir", "/tmp/er_push"
        )
        tmp_dir = f"{tmp_base}_{self.entity_type}"
        os.makedirs(tmp_dir, exist_ok=True)

        self._prepare_build_context(tmp_dir)
        dockerfile_path = self.write_serving_dockerfile(tmp_dir)

        # Build — tag both versioned and :latest
        run_cmd(
            f"docker build "
            f"-f {dockerfile_path} "
            f"-t {image_uri} "
            f"-t {image_latest} "
            f"{tmp_dir}"
        )

        # Push both tags
        run_cmd(f"docker push {image_uri}")
        run_cmd(f"docker push {image_latest}")

        print(f"[Push] Image pushed: {image_uri}")
        return image_uri

    # ------------------------------------------------------------------
    # Rollback log
    # ------------------------------------------------------------------

    def save_registry_log(
        self,
        version:   str,
        previous:  str | None,
        image_uri: str,
        metrics:   dict,
    ) -> str:
        log_data = {
            "entity_type":      self.entity_type,
            "version":          version,
            "previous_version": previous,
            "image_uri":        image_uri,
            "rollback_uri":     f"{self.image_base}:{previous}" if previous else None,
            "pushed_at":        datetime.utcnow().isoformat(),
            "metrics":          metrics,
            "rollback_cmd": (
                f"gcloud artifacts docker tags add "
                f"{self.image_base}:{previous} {self.image_base}:latest "
                f"--project={self.project}"
            ) if previous else "no previous version",
        }

        log_path = os.path.join(self.results_dir, "registry_log.json")
        with open(log_path, "w") as f:
            json.dump(log_data, f, indent=2)
        print(f"[Push] Registry log → {log_path}")
        return log_path

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------

    def push(self) -> str:
        print(f"\n[Push] Starting for entity_type={self.entity_type}")

        self.check_model_exists()
        metrics           = self.check_quality_gate()
        version, previous = self.get_next_version()
        image_uri         = self.build_and_push(version)
        log_path          = self.save_registry_log(version, previous, image_uri, metrics)

        # MLflow — use env var override (Vertex AI component) or config value
        mlflow.set_tracking_uri(
            os.environ.get("MLFLOW_TRACKING_URI")
            or self.config["mlflow"]["tracking_uri"]
        )
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
                "image_uri":        image_uri,
                "version":          version,
                "previous_version": previous or "none",
            })
            mlflow.log_metrics(metrics)

        print(f"\n[Push] Done")
        print(f"  Image   : {image_uri}")
        print(f"  Version : {version}")
        print(f"  Previous: {previous or 'none'}")

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
        RegistryPusher(config=config, entity_type=entity_type).push()

    print("\n" + "=" * 70)
    print("PUSH COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()