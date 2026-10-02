"""Données partagées par les tests des DAGs bucket (importées, pas des fixtures)."""


class FakeRow(dict):
    """Ligne ORM factice : supporte ``dict(row)`` et ``row.attribut``."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


REALM = {
    "name": "realm-a",
    "realm_apcode_details": [{"apcode": "AP1"}],
    "wklapp_account_number": "wk-123",
}

COS_INSTANCE = FakeRow(
    subscription_id="cos-sub",
    crn="crn:cos",
    name="cos-a",
    environment="dev",
    context={"realm": "realm-a", "app_code": "AP1", "wklapp_account_name": "wklapp-a"},
)

BACKUP_VAULT = {"subscription_id": "bv-sub", "crn": "crn:bv"}

ACCOUNT_CRNS = {"cloudlogs": "crn:logs", "encryption_key": "crn:kms"}

SECRETS = {
    "vault_read_token": "rt",
    "vault_read_addr": "ra",
    "vault_write_token": "wt",
    "vault_write_addr": "wa",
    "gitlab_token": "gl",
}

TF_OUTPUTS = {"bucket_name": {"value": "bucket-a"}, "bucket_crn": {"value": "crn:bucket"}}
