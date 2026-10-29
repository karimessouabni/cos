"""bucket : période de grâce du clean (ADR 0003) et unité de rétention (ADR 0001)

- clean_requested_at, clean_execute_at : horodatages avec fuseau (programmation du clean)
- clean_cbr_workspace_id : workspace Schematics de la quarantaine, tant qu'elle est posée
- retention_unit : unité saisie par le client ("days" / "years"), les bornes restent en jours
- clean_status : accepte NULL (l'ORM l'écrivait déjà à None à l'insertion)

Revision ID: 20261008_clean_grace
Revises: 68d62b4ffe7c (schéma initial, stampé sur int / pprod / prod)
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from alembic import op

revision = "20261008_clean_grace"
down_revision = "68d62b4ffe7c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("bucket", sa.Column("clean_requested_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.add_column("bucket", sa.Column("clean_execute_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.add_column("bucket", sa.Column("clean_cbr_workspace_id", sa.Text(), nullable=True))
    op.add_column("bucket", sa.Column("retention_unit", sa.Text(), nullable=True))
    op.alter_column("bucket", "clean_status", existing_type=sa.Text(), nullable=True)


def downgrade() -> None:
    op.alter_column("bucket", "clean_status", existing_type=sa.String(length=20), nullable=False)
    op.drop_column("bucket", "retention_unit")
    op.drop_column("bucket", "clean_cbr_workspace_id")
    op.drop_column("bucket", "clean_execute_at")
    op.drop_column("bucket", "clean_requested_at")
