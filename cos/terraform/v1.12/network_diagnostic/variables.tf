variable "region" {
  description = "Région des endpoints COS à résoudre."
  type        = string
  default     = "eu-fr2"
}

variable "extra_hosts" {
  description = "Noms supplémentaires à résoudre : l'hôte du virtual_server_endpoint d'un bucket, ceux de ibm_endpoints.json."
  type        = list(string)
  default     = []
}
