"""``provided`` : un objet JSON envoyé vide vaut « non fourni »."""
import pytest

from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.bucket_retention import BucketRetention
from cos_service.schemas.payload_object import provided


@pytest.mark.parametrize("empty", [
    None,
    {},
    {"retention_enabled": None, "default_days": None},
    BucketRetention(),
    BucketRetention.model_validate({}),
    BucketRetention.model_validate({"retention_enabled": None, "default_days": None, "maximum_years": None}),
    BucketBackup(),
], ids=["none", "empty-dict", "all-null-dict", "retention-default", "retention-{}", "retention-all-null", "backup-empty"])
def test_an_empty_object_is_not_provided(empty):
    assert provided(empty) is None


@pytest.mark.parametrize("sent", [
    {"backup_enabled": False},
    BucketRetention.model_validate({"default_days": 30, "minimum_days": 10, "maximum_days": 60}),
    BucketRetention.model_validate({"retention_enabled": False}),
    BucketRetention.model_validate({"default": 30, "minimum": 10, "maximum": 60}),  # format historique
    BucketBackup(backup_enabled=False),
], ids=["dict-false", "retention-days", "retention-disabled", "retention-legacy", "backup-disabled"])
def test_any_value_even_false_is_a_request(sent):
    """Un ``false`` explicite est une demande : l'objet est gardé tel quel."""
    assert provided(sent) is sent
