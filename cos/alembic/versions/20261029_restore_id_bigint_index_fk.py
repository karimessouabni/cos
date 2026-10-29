"""backup_vault_restore : id en bigint, index sur le bucket cible, FK vers backup_vault

Première révision réellement exécutée après le stamp du schéma initial. Elle
ne contient que les écarts que ``alembic check`` laissait entre les modèles et
pprod une fois les modèles alignés sur la base :

- ``id`` est ``varchar(50)`` en base avec des valeurs numériques ("1", "2", "3") :
  conversion en bigint, défaut sur la séquence, séquence recalée sur le max ;
- ``idx_restore_target_bucket`` sur ``target_bucket_crn`` ;
- clé étrangère ``backup_vault_subscription_id -> backup_vault.subscription_id``.

Avant de l'appliquer sur un environnement, vérifier qu'aucune ligne ne la fera
échouer (voir ``PRECHECK_SQL`` ci-dessous).

Revision ID: 20261029_restore_id
Revises: 20261008_clean_grace
Create Date: 2026-10-29
"""
import sqlalchemy as sa
from alembic import op

revision = "20261029_restore_id"
down_revision = "20261008_clean_grace"
branch_labels = None
depends_on = None

SEQUENCE = "backup_vault_restore_id_seq"

PRECHECK_SQL = """
-- ids non numériques (la conversion échouerait) :
select id from backup_vault_restore where id !~ '^[0-9]+$';
-- restaurations orphelines (la FK échouerait) :
select r.id, r.backup_vault_subscription_id from backup_vault_restore r
  left join backup_vault v on v.subscription_id = r.backup_vault_subscription_id
 where v.subscription_id is null;
"""


def upgrade() -> None:
    op.execute(f"CREATE SEQUENCE IF NOT EXISTS {SEQUENCE}")
    op.alter_column(
        "backup_vault_restore", "id",
        existing_type=sa.String(length=50), type_=sa.BigInteger(), existing_nullable=False,
        postgresql_using="id::bigint",
    )
    op.execute(f"ALTER TABLE backup_vault_restore ALTER COLUMN id SET DEFAULT nextval('{SEQUENCE}')")
    op.execute(f"ALTER SEQUENCE {SEQUENCE} OWNED BY backup_vault_restore.id")
    op.execute(f"SELECT setval('{SEQUENCE}', COALESCE((SELECT MAX(id) FROM backup_vault_restore), 0) + 1, false)")

    op.create_index("idx_restore_target_bucket", "backup_vault_restore", ["target_bucket_crn"])
    op.create_foreign_key(
        "backup_vault_restore_backup_vault_subscription_id_fkey",
        "backup_vault_restore", "backup_vault",
        ["backup_vault_subscription_id"], ["subscription_id"],
    )


def downgrade() -> None:
    op.drop_constraint("backup_vault_restore_backup_vault_subscription_id_fkey", "backup_vault_restore", type_="foreignkey")
    op.drop_index("idx_restore_target_bucket", table_name="backup_vault_restore")
    op.execute("ALTER TABLE backup_vault_restore ALTER COLUMN id DROP DEFAULT")
    op.alter_column(
        "backup_vault_restore", "id",
        existing_type=sa.BigInteger(), type_=sa.String(length=50), existing_nullable=False,
        postgresql_using="id::text",
    )
