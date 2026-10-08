"""bucket : période de grâce du clean (ADR 0003) et unité de rétention (ADR 0001)

- clean_requested_at, clean_execute_at : horodatages avec fuseau (programmation du clean)
- clean_cbr_workspace_id : workspace Schematics de la quarantaine, tant qu'elle est posée
- retention_unit : unité saisie par le client ("days" / "years"), les bornes restent en jours
- clean_status : accepte NULL (l'ORM l'écrivait déjà à None à l'insertion)

[À COMPLÉTER] ``down_revision`` doit être la tête actuelle de votre chaîne
(``alembic heads``) : ce dépôt ne contient pas vos autres migrations.

Revision ID: 20261008_clean_grace
Revises: <HEAD>
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from alembic import op

revision = "20261008_clean_grace"
down_revision = "<HEAD>"  # [À COMPLÉTER] alembic heads
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("bucket", sa.Column("clean_requested_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.add_column("bucket", sa.Column("clean_execute_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.add_column("bucket", sa.Column("clean_cbr_workspace_id", sa.String(length=100), nullable=True))
    op.add_column("bucket", sa.Column("retention_unit", sa.String(length=5), nullable=True))
    op.alter_column("bucket", "clean_status", existing_type=sa.String(length=20), nullable=True)


def downgrade() -> None:
    op.alter_column("bucket", "clean_status", existing_type=sa.String(length=20), nullable=False)
    op.drop_column("bucket", "retention_unit")
    op.drop_column("bucket", "clean_cbr_workspace_id")
    op.drop_column("bucket", "clean_execute_at")
    op.drop_column("bucket", "clean_requested_at")
