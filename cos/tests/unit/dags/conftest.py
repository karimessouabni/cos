"""Fixtures partagées par les tests des DAGs bucket.

Les constantes (``REALM``, ``COS_INSTANCE``...) sont dans ``support.py`` et
s'importent ; les fixtures, elles, sont fournies ici par pytest, sans import
(un import de fixture redéfinit le nom du paramètre : ruff F811).
"""
from unittest.mock import MagicMock

import pytest

from cos_service.schemas.subscription_status import SubscriptionStatus
from tests.unit.dags.support import ACCOUNT_CRNS, BACKUP_VAULT, COS_INSTANCE, REALM, SECRETS


@pytest.fixture
def make_payload(dag):
    def factory(**overrides):
        fields = dict(
            realm="realm-a",
            apcode="AP1",
            environment="dev",
            region="eu-de",
            subscription_id="sub-1",
            cos_instance="co21000001",
            storage_class=dag.module.BucketCreatePayload.StorageClass.STANDARD,
        )
        fields.update(overrides)
        return dag.module.BucketCreatePayload(**fields)

    return factory


@pytest.fixture
def state_manager():
    manager = MagicMock(name="state_manager")
    manager.get_subscription.return_value.description = "my bucket"
    return manager


@pytest.fixture
def happy_services(services):
    """Mocks configurés pour une demande valide."""
    services.contextService.get_realm.return_value = REALM
    services.contextService.get_apcodes.return_value = ["AP1"]
    services.contextService.get_account_instances_crn.return_value = ACCOUNT_CRNS
    services.cosService.get_cos_instance_by_name.return_value = COS_INSTANCE
    services.cosService.get_cos_instance_status.return_value = SubscriptionStatus.ACTIVE.value
    services.backup_vault_service.get_backup_vault_by_name.return_value = BACKUP_VAULT
    services.vault_service.get_vault_secrets.return_value = SECRETS
    return services
