"""Base déclarative commune aux modèles (SQLAlchemy 1.4 et 2.x)."""
from sqlalchemy import BigInteger, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# Identifiant des tables à séquence : ``bigint`` en PostgreSQL (comme la base réelle),
# ``INTEGER`` sur SQLite, le seul type que SQLite auto-incrémente dans les tests.
BigId = BigInteger().with_variant(Integer(), "sqlite")
