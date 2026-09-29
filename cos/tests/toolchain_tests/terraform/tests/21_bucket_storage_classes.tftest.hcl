# Une création par classe de stockage, en un seul run (la classe n'est pas
# modifiable ensuite : un second run sur le même module la remplacerait).

run "cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} storage classes"
  }
}

run "one_bucket_per_storage_class" {
  module { source = "./modules/buckets" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} storage class"
    cos_instance = run.cos.name
    buckets = {
      vault = { storage_class = "vault" }
      cold  = { storage_class = "cold" }
      smart = { storage_class = "smart" }
    }
  }

  assert {
    condition     = length(output.names) == 3 && alltrue([for n in values(output.names) : n != ""])
    error_message = "Les trois buckets n'ont pas tous été créés."
  }
  assert {
    condition     = { for k, p in output.payloads : k => p.storage_class } == { vault = "vault", cold = "cold", smart = "smart" }
    error_message = "storage_class non respectée."
  }
}
