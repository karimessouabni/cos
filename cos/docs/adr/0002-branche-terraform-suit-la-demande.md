# ADR 0002 – Schematics clone la branche sur laquelle le DAG tourne

**Statut** : accepté · **Date** : 2026-09-28

## Contexte

Le dépôt `cos` contient à la fois le code des DAGs et le Terraform qu'ils exécutent via
IBM Schematics. Schematics ne reçoit pas ce Terraform : il le clone lui-même depuis le
dépôt, sur une branche que le DAG lui indique (`VCS.branch`).

Sur l'orchestrateur, les branches (`main` et les branches de feature) sont enregistrées
dans l'interface, et la gateway qui lance un DAG précise à chaque demande la branche du
produit à exécuter (`product_branch`). Plusieurs développeurs testent donc en même temps
leurs branches sur le même INT, chacun avec son propre code de DAG.

Jusqu'ici, la branche clonée par Schematics était fixée par environnement, avec un nom de
branche de feature codé en dur pour l'INT. Une seule feature Terraform pouvait être testée
à la fois, et le DAG d'une branche pouvait exécuter le Terraform d'une autre.

## Décision

1. **Schematics clone la branche sur laquelle le DAG tourne.** `settings_for(env,
   product_branch)` reçoit la branche de la demande et la transmet au VCS du workspace.
   DAG et Terraform sont toujours à la même version.
2. **La branche de la demande est `payload.product_branch`**, le champ que le client (ou
   la gateway) envoie avec chaque demande à côté du `payload` produit. Le DAG le passe tel
   quel au service Schematics. Rien n'est écrit dans le code ni posé à la main.
3. **Défauts par environnement alignés sur les branches de la CI** quand la demande ne
   porte pas de branche : `main` (int), `preprod` (pprod), `prod` (prod).
4. **Garde-fou en pprod et prod** : toute branche autre que celle de l'environnement est
   refusée avant tout appel Schematics, qu'elle vienne de la demande ou d'une surcharge.
5. **Surcharges conservées comme roue de secours, INT seulement** : Airflow Variable
   `cos_tf_branch`, puis variable d'environnement `COS_TF_BRANCH`. Même mécanisme pour
   `TF_LOG` (`cos_tf_log_level`, `COS_TF_LOG_LEVEL`), avec un défaut par environnement
   (`DEBUG`, `INFO`, `ERROR`).
6. **La CI ne déploie ni ne surcharge rien.** Elle garantit ce qui arrive sur une branche
   (tests, standards, aucun nom de branche de feature en dur, miroir pour les scans).

## Conséquences

- Aucune opération manuelle pour tester une branche sur l'INT : enregistrer la branche
  dans l'orchestrateur, lancer la demande dessus, Schematics suit.
- Plusieurs développeurs testent en parallèle sans se marcher dessus.
- Un workspace créé sur une branche de feature garde cette branche dans son VCS : les
  mises à jour suivantes du même bucket sur l'INT continuent de la cloner tant qu'elle
  existe. Supprimer les buckets de test avant de supprimer la branche.
- Le champ `product_branch` fait partie du payload de base fourni par
  `bp2i_airflow_library` ; il est aussi porté par la doublure de test
  (`tests/unit/stubs/bp2i.py`).
