output "resolved_ips" {
  description = "Nom -> adresses IPv4 vues par l'agent Schematics."
  value       = { for host, record in data.dns_a_record_set.resolved : host => sort(record.addrs) }
}

output "private_ips" {
  description = "Les seuls noms qui résolvent vers une adresse privée (10/8, 172.16/12, 192.168/16, 166.8/14 IBM)."
  value = {
    for host, record in data.dns_a_record_set.resolved : host => sort(record.addrs)
    if length([for ip in record.addrs : ip if can(regex("^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|166\\.(8|9|10|11)\\.)", ip))]) > 0
  }
}
