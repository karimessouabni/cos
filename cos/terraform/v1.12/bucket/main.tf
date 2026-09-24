################################################################################
# GET APIKEY FROM VAULT WKLAPP
################################################################################
# [RECONSTITUTION] Reconstitué depuis les captures PyCharm (159 lignes, toutes
# visibles). Un seul changement fonctionnel par rapport à l'original, sur
# backup_policies (voir le commentaire du bloc) ; random_id.backup_policy,
# jamais référencé, est retiré.
module "vault" {
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//read?ref=v2.0.0"
  secret_path = "${local.vault_namespace}/${local.vault_secret_path_account}"
  providers = {
    vault = vault.read
  }
}

module "vault_naming" {
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//read?ref=v2.0.0"
  secret_path = "${local.vault_namespace}/${local.vault_secret_naming_path}"
  providers = {
    vault = vault.read
  }
}

module "naming_bucket" {
  source = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-naming.git//paas?ref=v3.0.0"
  metier = "BP2I"
  #cloud_type    = "2"
  cloud_type     = var.cloud_type
  service_name   = "bu"
  cloud_provider = "i"
}

module "iam_service_id" {
  for_each                   = toset(local.service_id_roles)
  source                     = "../modules/terraform-ibm-iam-service-id-main"
  iam_service_id_name        = "sid-${module.naming_bucket.name}-${each.key}" #module.naming_bucket.name
  iam_service_id_description = "${each.key} ServiceID for Bucket to use for resource key credentials"
  iam_service_id_policies = {
    policy = {
      roles = [each.key]
      resource_attributes = [
        {
          name  = "resourceType"
          value = "bucket"
        },
        {
          name  = "serviceName"
          value = "cloud-object-storage"
        },
        {
          name     = "resource"
          value    = module.naming_bucket.name
          operator = "stringEquals"
        }
      ]
    }
  }
}

module "bucket" {
  source = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-cos.git?ref=10.14.8"
  #source                             = "../modules/terraform-module-cos"
  bucket_name                         = module.naming_bucket.name
  add_bucket_name_suffix              = false
  management_endpoint_type_for_bucket = var.management_endpoint_type_for_bucket
  region                              = var.region
  bucket_storage_class                = var.bucket_storage_class
  #cross_region_location              = var.cross_region_location
  archive_days = null
  #expire_days                        = var.expire_days
  expire_days = null
  #monitoring_crn                     = var.sysdig_crn
  #activity_tracker_crn               = var.activity_tracker_crn
  create_cos_instance           = false
  existing_cos_instance_id      = var.cos_instance_crn
  skip_iam_authorization_policy = true # Required since cos_bucket1 creates the IAM authorization policy
  kms_key_crn                   = var.kms_key_crn
  retention_enabled             = var.retention.retention_enabled
  retention_default             = var.retention.default
  retention_maximum             = var.retention.maximum
  retention_minimum             = var.retention.minimum
  object_versioning_enabled     = var.object_versioning_enabled
  object_locking_enabled        = var.object_locking_enabled
  hard_quota                    = 0
  object_lock_duration_days     = var.object_lock_duration_days
  object_lock_duration_years    = var.object_lock_duration_years

  resource_keys = !var.enable_custom_permissions ? {} : {
    0 : {
      name                      = "hmac-${module.naming_bucket.name}-reader"
      generate_hmac_credentials = true
      role                      = local.bucket_role_none #"Reader"
      service_id_crn            = module.iam_service_id["Reader"].service_id_crn
    },
    1 : {
      name                      = "hmac-${module.naming_bucket.name}-writer"
      generate_hmac_credentials = true
      role                      = local.bucket_role_none #"Writer"
      service_id_crn            = module.iam_service_id["Writer"].service_id_crn
    }
  }

  # Le module indexe son for_each de ibm_cos_backup_policy sur policy_name.
  # Une clé de for_each doit être connue au plan : le nom du bucket, produit
  # par le module de naming à l'apply, ne peut donc pas y figurer sur un
  # create ("Invalid for_each argument"). Une policy n'a besoin d'être unique
  # que dans son bucket, le nom du bucket n'apporte rien.
  # Ancienne valeur : "${module.naming_bucket.name}-bp-${var.initial_delete_after_days}d".
  # Les policies déjà déployées sous l'ancien nom sont recréées au prochain
  # apply de leur bucket (changement de clé dans le state).
  backup_policies = !var.backup_enabled ? [] : [
    {
      policy_name               = "bp-${var.initial_delete_after_days}d"
      target_backup_vault_crn   = var.target_backup_vault_crn
      initial_delete_after_days = var.initial_delete_after_days
    }
  ]
}

data "ibm_is_virtual_endpoint_gateway" "vpe-cos" {
  count = var.region == "eu-de" || var.region == "eu-fr2" && (var.wkld_account_sub_type == "PO-VITAL" || var.wkld_account_sub_type == "VITAL") ? 1 : 0
  name  = "vpe-cos"
}

resource "ibm_is_virtual_endpoint_gateway_resource_binding" "is_virtual_endpoint_gateway_resource_binding_instance" {
  count               = var.region == "eu-de" || var.region == "eu-fr2" && (var.wkld_account_sub_type == "PO-VITAL" || var.wkld_account_sub_type == "VITAL") ? 1 : 0
  endpoint_gateway_id = data.ibm_is_virtual_endpoint_gateway.vpe-cos[0].id
  name                = "${module.naming_bucket.name}"
  target {
    crn = module.bucket.bucket_crn
  }
}

module "vault_write_writer" {
  count       = var.enable_custom_permissions ? 1 : 0
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//write?ref=v2.0.0"
  secret_path = "${var.app_code}/objsto/${var.cos_instance_name}/${module.naming_bucket.name}/writer"
  map_secrets = local.bucket_writer_credentials
  providers = {
    vault = vault.write
  }
}

module "vault_write_reader" {
  count       = var.enable_custom_permissions ? 1 : 0
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//write?ref=v2.0.0"
  secret_path = "${var.app_code}/objsto/${var.cos_instance_name}/${module.naming_bucket.name}/reader"
  map_secrets = local.bucket_reader_credentials
  providers = {
    vault = vault.write
  }
}
