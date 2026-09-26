# ADR 0001 – Rétention en jours et en années sans rupture du contrat v1

**Statut** : accepté · **Date** : 2026-09-26

## Contexte

Le plafond de rétention d'un bucket passe à cinq ans. Saisir 1825 jours est peu lisible,
d'où l'ajout d'une unité "années" : `default_years`, `minimum_years`, `maximum_years`,
et les choix `retention_daily` / `retention_yearly` (idem pour l'object lock).

Des clients utilisent déjà le produit avec le format historique en jours implicites
(`default`, `minimum`, `maximum`). Leur code Terraform passe par le module `dmzrasc/cos`
et le provider `orchestrator`, qui transportent le `payload` sans l'interpréter : seul le
DAG (`BucketCreatePayload` / `BucketRetention`) connaît ces champs.

## Décision

1. **Le contrat reste en `v1`.** Ajouter une unité est additif ; une `v2` (DAGs
   `cos.bucket.v2.*` + ressource `cosbucket_v2` + majeure du module) est réservée à un
   changement de sémantique.
2. **Les deux formats sont acceptés** par `BucketRetention`. Le format historique est
   recopié vers `*_days` à la validation, marqué `deprecated` dans le schéma et journalisé
   en warning avec les valeurs reçues. Mélanger les deux formats est refusé.
3. **Le jour est l'unité canonique** de la base (`retention_default/minimum/maximum`) et
   du Terraform interne. Les années sont converties en leur équivalent exact en jours à la
   date de la demande (bissextiles comprises), après vérification des bornes dans l'unité
   saisie par `BucketRetention._validate`. Le service ne re-vérifie pas les bornes à la
   création ; il les vérifie encore à la mise à jour, sur les valeurs fusionnées avec la
   ligne bucket.
4. **Le state ne renomme aucune clé.** `retention.default/minimum/maximum` restent en
   jours ; on y ajoute `unit` et les bornes telles que le client les a envoyées
   (`default_years`…), pour qu'un client en années relise ce qu'il a écrit. Les DAGs
   `create` et `update` passent tous deux par `retention_state_for_client`.
5. **Module Terraform `dmzrasc/cos`** : pas de bump obligatoire tant que `cos_buckets`
   n'est pas typé sur `retention` ; mineur (`2.1.0`) si des `optional()` sont ajoutés ;
   `3.0.0` seulement le jour du retrait de l'ancien format.

## Retrait de l'ancien format

Le format historique sera retiré après une période d'annonce (cible : six mois, à
confirmer avec les clients identifiés en base par `retention_enabled = true`). Jusque-là :

- le warning `Legacy retention payload` permet de lister les souscriptions concernées ;
- les tests `tests/test_immutability_service_compat.py` garantissent que l'ancien format
  produit exactement la même immutabilité qu'avant.

## Conséquences

- Aucun client existant n'a de changement à faire pour continuer à créer ou mettre à jour
  des buckets.
- Point à vérifier dans le provider `orchestrator` : si son Read compare le `payload` au
  state, un client en années pourrait voir un drift sur `default` (730 vs 2). L'écho des
  bornes saisies dans le state est là pour ça ; le test "second plan sans changement" sur
  la toolchain doit le confirmer.
- À prévoir en base : une colonne `retention_unit` pour restituer l'unité aussi sur les
  lectures qui ne passent pas par un payload (update partiel, refresh).
- Point ouvert, conversion dépendante de la date : `2 ans` vaut 730 ou 731 jours selon le
  jour de la demande, et le plafond en jours vaut 1826 ou 1827. Deux demandes identiques à
  des dates différentes stockent des valeurs différentes, et un client en années peut voir
  un écart d'un jour entre son state et une nouvelle évaluation. Alternatives : figer
  365 jours par an, ou stocker l'unité et la valeur saisies et ne convertir qu'au moment
  d'écrire le Terraform.
- Point ouvert, égalité des bornes : le schéma accepte `minimum = default = maximum` ;
  `check_retention_bounds` (mise à jour) exige `default < maximum` strictement. À aligner
  dans un sens ou dans l'autre.
