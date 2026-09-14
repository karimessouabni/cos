# Structure du projet `cos` (~/PycharmProjects/cos)

Reconstituée depuis les captures PyCharm (branche `feature/update-retention-to-5-years`).
Les éléments marqués `(déduit)` ne sont pas visibles dans l'explorateur mais sont
référencés par les imports des fichiers reconstitués.

```
cos/
├── .venv/
├── cos_service/
│   ├── __init__.py
│   ├── dags/
│   │   ├── __init__.py
│   │   ├── backup_vault/
│   │   ├── bucket/
│   │   │   └── v1/
│   │   │       ├── cos.bucket.v1.clean.py
│   │   │       ├── cos.bucket.v1.create.py                      ← reconstitué
│   │   │       ├── cos.bucket.v1.create_lifecycle_policy_rule.py
│   │   │       ├── cos.bucket.v1.delete.py
│   │   │       ├── cos.bucket.v1.delete_lifecycle_policy_rule.py
│   │   │       ├── cos.bucket.v1.force_clean.py
│   │   │       ├── cos.bucket.v1.refresh_restore_ranges.py
│   │   │       ├── cos.bucket.v1.restore.py
│   │   │       ├── cos.bucket.v1.update.py
│   │   │       └── cos.bucket.v1.update_lifecycle_policy_rule.py
│   │   ├── bucket_migration/
│   │   ├── cos/
│   │   ├── product/
│   │   └── v1/
│   ├── dependencies/
│   ├── models/
│   ├── repository/
│   ├── schemas/
│   │   ├── bucket_backup.py          (déduit)  BucketBackup
│   │   ├── bucket_retention.py                 BucketRetention, DAYS, YEARS, MAX_RETENTION_YEARS, max_retention, _RETENTION_KEYS
│   │   ├── immutability.py                      ← reconstitué (enum Immutability)
│   │   ├── status.py                 (déduit)  Status
│   │   └── subscription_status.py    (déduit)  SubscriptionStatus
│   ├── services/
│   │   ├── backup_vault_service.py   (déduit)  get_backup_vault_by_name, get_backup_vault_by_sub_id
│   │   ├── bucketService.py          (déduit)  get_bucket_by_sub_id, process_bucket_creation, update_bucket_*, complete_bucket_create
│   │   ├── contextService.py         (déduit)  get_realm, get_apcodes, get_account_instances_crn
│   │   ├── cosService.py             (déduit)  get_cos_instance_by_name, get_cos_instance_status
│   │   ├── immutability_service.py              ← reconstitué
│   │   ├── schematics_service.py                ← reconstitué + corrigé (create_or_update_ws, update_ws, update_ws_variables, run_workspace)
│   │   ├── vault_service.py          (déduit)  get_vault_secrets
│   │   └── workspaceService.py       (déduit)  update_bucket_workspace, build_bucket_workspace_details
│   ├── sql/                                     ← scripts SQL gérés à la main (cible Alembic)
│   └── utils/
├── terraform/
│   └── v1.12/bucket/main.tf          (déduit du tf_directory + onglet main.tf)
├── tests/
├── .gitignore
├── __init__.py
├── poetry.lock
├── poetry.lock.txt
├── pyproject.toml
├── pyproject.toml.txt
├── README.md
└── requirements-dev.txt
```

Dépendances externes visibles : `bp2i_airflow_library` (dag, dependencies, schemas,
config, exceptions), `bp2i_terraform` (backends.schematics), `sqlalchemy` (SASession),
`python-dateutil`. Le projet est un ensemble de DAGs Airflow (`@product_action` / `@step`)
qui pilotent Terraform via IBM Schematics et persistent l'état dans PostgreSQL via SQLAlchemy.
