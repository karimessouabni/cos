# ADR 0003 : période de grâce et quarantaine avant le clean d'un bucket

- Statut : accepté, mis en œuvre par étapes
- Date : 2026-10-06
- Étapes : **v1** (branche `feature/clean-grace-period-v1`) grâce réglable en
  minutes, annulation, contrôle des verrous par le listing, sans quarantaine ;
  **v2** (`main`) la même chose plus la quarantaine CBR par un workspace séparé.

## Contexte

Le clean vide un bucket par une règle d'expiration S3, c'est irréversible. Une
demande lancée par erreur détruit les données d'une application sans délai.
Le besoin : un délai de rétractation de sept jours, annulable, et pendant ce
délai, plus aucun accès au bucket pour le client, de façon à ce qu'une erreur
se voie tout de suite (l'application tombe en 403) et non sept jours plus tard
(les données ont disparu).

Contraintes établies :

- les clients accèdent au bucket par l'URL publique, avec une clé API ou des
  identifiants HMAC, avec ou sans proxy d'entreprise ; leurs adresses IP ne
  les distinguent pas de l'orchestrateur ;
- Airflow liste le bucket par son endpoint direct (`virtual_server_endpoint`) ;
  Schematics applique le Terraform du bucket depuis le compte hub, avec des
  accès croisés vers le compte workload qui possède le bucket ;
- une rétention IBM ou un Object Lock ne se retire jamais d'un bucket
  (documentation IBM, `immutable.md` et `object-lock.md`).

## Décision

1. **Aucune configuration destructrice pendant la grâce.** Le compte à
   rebours est côté orchestrateur (`DateTimeSensorAsync`, différé, aucun
   worker occupé). La règle d'expiration n'est posée qu'après la décision
   finale. Une variante « règle d'expiration datée à J+7 » a été écartée :
   si la levée échoue à J+6, COS détruit quand même.
2. **La quarantaine est une règle Context-Based Restrictions sur le seul
   bucket**, qui n'autorise que les endpoints `direct` / `private` depuis la
   zone réseau de l'orchestrateur (VPC de la plateforme, référence de service
   Schematics). Les clients, sur l'endpoint public, reçoivent 403 quel que
   soit leur proxy. Le discriminant est le type d'endpoint, pas l'adresse IP.
3. **La règle CBR est portée par un workspace Schematics séparé du bucket**
   (`terraform/v1.12/bucket_quarantine`, nom `ws_cbr_bucket_<subscription>`,
   identifiant gardé en base dans `clean_cbr_workspace_id`). Son state ne
   contient que la zone et la règle : son refresh n'appelle que l'API CBR,
   qu'une règle sur COS ne bloque jamais, il reste donc pilotable quoi que la
   règle bloque, et la lever est toujours possible. Le bucket n'y est jamais
   lu, son nom et son instance passent en variables depuis la base. Une
   première version mettait la règle dans le workspace du bucket : son refresh
   relisait le bucket sous quarantaine, avec un risque d'auto-verrouillage si
   la zone était mal taillée. Le workspace est créé à la pose et détruit à la
   levée (fin du clean, annulation, ou suppression du bucket par `delete`),
   ce qui tient aussi le plafond de règles CBR par compte. Même identité et
   mêmes accès croisés hub vers workload que les workspaces de buckets ; seul
   prérequis : le rôle d'administration CBR sur cette identité. Sortie de
   secours : l'identifiant de la règle est en sortie du workspace, `ibmcloud
   cbr rule-delete <id>` puis suppression du workspace.
4. **La décision est une mise à jour conditionnelle en base**
   (`transition_bucket_clean` : `scheduled -> in_progress`, rowcount 1). Elle
   départage l'exécution et une annulation arrivant au même instant : la base
   ne laisse passer que la première. Si le clean a été annulé, la demande est
   déclinée et rien n'est supprimé.
5. **L'annulation est une action à part**, `cos.bucket.v1.cancel_clean`,
   exposée par le provider et le portail. Acceptée sur `scheduled` avant la
   date d'exécution, sur `failed` (rendre le bucket accessible après un
   échec) et sur `cancelled` (idempotente : relance seulement la levée de la
   quarantaine). Refusée dès que le clean a commencé : plus de retour en
   arrière une fois la règle posée.
6. **Les buckets à rétention ou Object Lock sont jugés sur une date, pas sur
   la base.** La documentation IBM garantit que l'expiration diffère les objets
   protégés plutôt que d'échouer : un bucket verrouillé n'est pas dangereux à
   nettoyer, il est lent. Un objet protégé par la politique du bucket est libre
   au plus tard à `LastModified + durée maximale` (`retention_maximum` en
   jours, ou la durée d'Object Lock). Un passage de listing, versions comprises
   pour l'Object Lock, donne le `LastModified` le plus récent, donc la borne
   (`latest_object_modification`, une requête par tranche de mille, aucun
   `HEAD`). Borne avant la fin de la grâce : accepté. Après : refusé avec la
   date à laquelle relancer. Les legal holds et les rétentions explicites plus
   longues que la politique ne sont pas vus par cette borne ; ils retardent
   alors le vidage sans le mettre en danger (sensor, puis timeout).
7. **La clé API COS ne traverse pas la grâce.** Elle est lue dans Vault pour
   le contrôle des verrous, puis relue après le délai pour le vidage : son bail
   peut être plus court que sept jours.

## Conséquences

- Nouvelles colonnes `bucket.clean_requested_at` et `bucket.clean_execute_at`
  (horodatage avec fuseau). `clean_status` prend les valeurs de
  `CleanStatus` : `scheduled`, `in_progress`, `success`, `failed`,
  `cancelled`. Les trois valeurs existantes sont inchangées.
- Colonne `bucket.clean_cbr_workspace_id` (texte, nullable). Réglages de la
  quarantaine (Airflow Variable, sinon variable d'environnement) :
  `cos_quarantine_allowed_vpc_crns` (VPC de l'orchestrateur, séparés par des
  virgules) et `cos_quarantine_enforcement_mode` (`report` pour valider la
  zone sur les premiers clients, puis `enabled`). Le workspace de quarantaine
  lit la clé API du compte workload dans Vault comme celui du bucket
  (`providers.tf` à aligner sur celui du bucket, absent de ce dépôt).
- Le state client porte `clean_status`, `clean_requested_at`,
  `clean_execute_at` et `quarantine`.
- La grâce est réglable en minutes (Airflow Variable `cos_clean_grace_minutes`,
  sinon `COS_CLEAN_GRACE_MINUTES`, défaut 10080 soit 7 jours) : les tests
  toolchain en INT la mettent à quelques minutes.
- Ce que le client écrit pendant la grâce est supprimé aussi : le state le dit
  (`clean_notice`) avec la date d'exécution. En v2 il ne peut plus écrire,
  la notice reste vraie pour ce qui passe avant la propagation de la règle.
- Le succès n'est posé qu'après la levée de la quarantaine. Un échec après la
  décision laisse le bucket en quarantaine et `failed` ; `cancel_clean` la lève.
- Points à surveiller en exploitation : la propagation d'une règle CBR prend
  quelques minutes (quarantaine et levée ne sont pas instantanées) ; le
  nombre de règles CBR par compte est plafonné (une par bucket en grâce,
  toujours retirée) ; un workload client hébergé dans un VPC listé dans
  `quarantine_allowed_vpc_crns` ne serait pas bloqué, la zone doit rester
  celle de l'orchestrateur ; le timeout Airflow du sensor de vidage est levé
  hors du `try` et ne pose pas `failed` (callback d'échec à ajouter si
  `step.sensor` le transmet).
- Reste à faire hors de ce dépôt : la migration des trois colonnes
  (`clean_requested_at`, `clean_execute_at`, `clean_cbr_workspace_id`), le
  `providers.tf` du module de quarantaine aligné sur celui du bucket, le rôle
  CBR sur l'identité Schematics, les deux réglages de quarantaine en INT, les
  actions `cancel_clean` dans le provider, et la notification du demandeur à
  la programmation et la veille de l'exécution. À refuser en v1.1 : `update`,
  `delete` et `restore` tant qu'un clean est programmé.
