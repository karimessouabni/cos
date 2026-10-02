"""Données partagées par les tests des DAGs bucket (importées, pas des fixtures)."""


class FakeRow(dict):
    """Ligne ORM factice : supporte ``dict(row)`` et ``row.attribut``."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class FakeModel:
    """Modèle renvoyé par le ReaderConnector (pydantic) : attributs + ``model_dump()``."""

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def model_dump(self) -> dict:
        return dict(self.__dict__)

    def __eq__(self, other):
        return isinstance(other, FakeModel) and self.__dict__ == other.__dict__


REALM_DICT = {
    "name": "realm-a",
    "status": 200,
    "realm_apcode_details": [{"apcode": "AP1"}],
    "wklapp_account_number": "wk-123",
}
# Ce que get_realm(reader) renvoie : un modèle à attributs ; validated["realm"] en est le model_dump().
REALM = FakeModel(**REALM_DICT)

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
