Entity-Resolution/
├── Data-Pipeline/                              # Airflow DAG for data generation
├── Model-Pipeline/
│   ├── pipeline.py                             # ML training pipeline
│   ├── scripts/
│   │   ├── serve.py                            # Model serving + prediction logging
│   │   ├── train.py                            # DeBERTa + LoRA training
│   │   └── evaluate.py                         # Model evaluation
│   └── config/
│       └── training_config.yaml                # Thresholds, notifications, GCP config
├── Monitoring-Pipeline/
│   ├── monitoring_pipeline.py                  # Vertex AI monitoring pipeline
│   ├── scripts/
│   │   └── monitor.py                          # Core monitoring logic (ModelMonitor)
│   ├── grafana/
│   │   └── er-monitoring-dashboard.json        # Grafana dashboard export
│   └── README.md                               # This file
├── Initial_Setup/                              # Terraform GCP resources
└── README.md                                   # Project overview
