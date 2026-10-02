# Structure du projet `cos` (~/PycharmProjects/cos)

Reconstituée depuis les captures PyCharm (branche `feature/update-retention-to-5-years`).
Les éléments marqués `(déduit)` ne sont pas visibles dans l'explorateur mais sont
référencés par les imports des fichiers reconstitués.

```
cos/
├── .venv/
├── cos-subscriptions/
│   ├── subscriptions_cleanup.py      ← nettoyage des souscriptions orchestrator (--delete, --on-error)
│   └── test_subscriptions_cleanup.py
├── tests/
│   ├── unit/                         ← tests unitaires sans les libs internes (voir tests/unit/README.md)
│   │   ├── conftest.py                   stubs installés seulement si le vrai module manque
│   │   ├── stubs/                        bp2i.py (step/depends), orm.py (sqlalchemy, modèles), schemas.py
│   │   ├── dags/                         test_bucket_create.py, _delete.py, _update.py, _restore.py, support.py, conftest.py
│   │   ├── schemas/                      test_bucket_retention.py (contrat des deux formats)
│   │   ├── services/
│   │   └── integration/                  DagBag réel (COS_TESTS_FORCE_STUBS=0 -m integration)
│   └── toolchain_tests/              ← scénarios Terraform contre la toolchain (voir tests/toolchain_tests/README.md)
├── docs/adr/                         ← décisions d'architecture (0001 rétention jours/années ; 0002 branche Terraform = branche de la demande)
├── .gitlab-ci.yml                    ← CI : standards des MR (changelog, Conventional Commit, .airflowignore), miroir ITG, scans CoE
├── .gitlab-ci.standards.yml          ← tests unitaires + garde-fou branche de feature dans schematics_service
├── .gitlab-requirements.yml   (déduit)  .base_job Poetry, stage install_requirements
├── pytest.ini
├── requirements-test.txt
├── .gitignore
├── __init__.py
├── poetry.lock
├── poetry.lock.txt
├── pyproject.toml
├── pyproject.toml.txt
├── README.md                         ← présentation vulgarisée du produit (diagrammes Mermaid)
└── requirements-dev.txt
```

Dépendances externes visibles : `bp2i_airflow_library` (dag, dependencies, schemas,
config, exceptions), `bp2i_terraform` (backends.schematics), `sqlalchemy` (SASession),
`python-dateutil`. Le projet est un ensemble de DAGs Airflow (`@product_action` / `@step`)
qui pilotent Terraform via IBM Schematics et persistent l'état dans PostgreSQL via SQLAlchemy.
