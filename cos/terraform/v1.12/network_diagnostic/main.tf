################################################################################
# Diagnostic réseau vu de l'agent Schematics
################################################################################
# Aucune ressource : uniquement des data sources DNS, lues au plan. Un
# « Generate plan » du workspace suffit, les résultats sont dans « Changes to
# Outputs ». Le résolveur utilisé est celui de l'agent, donc du cluster IKS de
# la BU : c'est exactement le chemin que prend Terraform vers COS et CBR.
#
# Lecture : si les noms COS résolvent vers une IP privée d'un VPE du compte
# hub, le hub a son propre chemin (zone CBR = VPC de l'agent). S'ils résolvent
# vers l'IP du VPE COS ADC partagé (via le Transit Gateway), hub et clients
# arrivent par le même VPE et aucune zone réseau ne les sépare.
#
# Un nom qui ne résout pas fait échouer le plan avec ce nom dans le message :
# c'est aussi une réponse (l'agent ne le voit pas).

locals {
  hosts = toset(concat([
    "s3.direct.${var.region}.cloud-object-storage.appdomain.cloud",
    "s3.private.${var.region}.cloud-object-storage.appdomain.cloud",
    "s3.${var.region}.cloud-object-storage.appdomain.cloud",
    "config.direct.cloud-object-storage.cloud.ibm.com",
    "iam.cloud.ibm.com",
    "private.iam.cloud.ibm.com",
    "cbr.cloud.ibm.com",
    "private.cbr.cloud.ibm.com",
  ], var.extra_hosts))
}

data "dns_a_record_set" "resolved" {
  for_each = local.hosts
  host     = each.value
}
