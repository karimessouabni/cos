# ADR 0005 : la période de grâce et la quarantaine sont portées par la suppression

- Statut : **proposé** (à décider après le lot 0, voir « Avant de coder »)
- Date : 2026-10-29
- Remplace : la section « Suppression du bucket (`delete`) » de l'ADR 0004 ;
  le reste des ADR 0003 et 0004 (grâce, quarantaine CBR dans un workspace
  séparé, décision atomique, contrôle des verrous, clé API relue après la
  grâce) est repris tel quel par la suppression.
- Méthode : quatre architectures indépendantes (grâce dans le delete,
  corbeille + purge planifiée, clean puis delete enchaînés, angle libre)
  confrontées par trois lectures (contrat orchestrateur et provider, sûreté
  et exploitation, produit et maintenance). Celle-ci est la gagnante, avec
  les greffes retenues des trois autres.

## En bref

Aujourd'hui, supprimer un bucket qui contient des données demande deux
actions : `clean` (sept jours de grâce, quarantaine, vidage) puis `delete`
(refusé tant que le bucket n'est pas vide, destruction immédiate). La
demande : **une seule action « supprimer »** qui pose la grâce et la
quarantaine, pendant laquelle le bucket **n'apparaît plus** dans la base et
reste **rétractable**, puis vide et détruit.

Décision : oui, la grâce doit vivre dans la demande `delete`, et pour une
raison de contrat plus que d'ergonomie. Pour un client Terraform, le seul
signal « le bucket existe-t-il encore ? » est l'état terminal de la demande
delete que le provider attend. Si la demande se termine avant la fin de la
grâce (variante « corbeille »), la ressource sort du state Terraform et la
rétractation devient impossible côté Terraform. Si elle reste ouverte
jusqu'à la décision, une rétractation la fait finir `CANCELLED` et la
ressource reste gérée. La demande delete doit donc **vivre toute la grâce**,
comme la demande clean et l'update planifié le font déjà
(`DateTimeSensorAsync` différé, aucun worker occupé).

« Déclencher le clean via le delete » n'est pas un enchaînement de DAGs
(une demande = un DAG, rien dans la bibliothèque n'autorise un DAG à lancer
une autre demande) : le DAG delete **embarque les étapes de vidage du
clean**, extraites dans un service partagé. Le DAG clean (vider sans
détruire) reste une action distincte, inchangée.

## Contexte et contraintes

- Les contraintes réseau de l'ADR 0004 tiennent : la quarantaine CBR bloque
  tout, l'orchestrateur l'ouvre quelques minutes pour agir. Rien de
  destructeur n'est posé avant la décision finale (ADR 0003, décision 1).
- Le provider orchestrator renvoie à chaque update toutes les propriétés du
  schéma du payload : **aucune option de grâce dans le payload**, elle serait
  renvoyée à chaque update et ferait un replace. La grâce est un réglage
  produit (`cos_delete_grace_minutes`, Airflow Variable sinon variable
  d'environnement, défaut 10080).
- Le workspace Schematics du bucket porte son state Terraform : il est
  conservé jusqu'au destroy. Le workspace de quarantaine est séparé et reste
  pilotable sous quarantaine (ADR 0003, décision 3).
- Les lecteurs de la base ne sont pas tous dans ce dépôt (portail, services
  du projet d'équipe) : « ne remonte plus » doit être porté par la base
  (colonnes et vue), pas par un filtre Python.

## Décision

### 1. Une seule demande, qui vit toute la grâce

Le DAG `cos.bucket.v1.delete` devient :

| # | Étape | Base | State client |
|---|---|---|---|
| 1 | `validate_request` | rien | rien |
| 2 | `schedule_delete` | `status=terminating`, `delete_status=scheduled`, `delete_requested_at`, `delete_execute_at`, `delete_requested_by` | `delete_status`, les dates, `delete_notice` |
| 3 | `quarantine_bucket` | `clean_cbr_workspace_id` | `quarantine: true` |
| 4 | `wait_for_grace_period` (`DateTimeSensorAsync` différé) | | |
| 5 | `claim_delete` (décision atomique) | `scheduled -> in_progress` | `delete_status: in_progress` |
| 6 | `get_cos_api_key` (relue après la grâce) | | |
| 7 à 11 | ouverture, listing, règle d'expiration, trace en base, refermeture | `delete_rule_posed_at` | |
| 12 | `wait_until_empty` (sensor quotidien, reschedule) | | |
| 13 | `lift_quarantine` | `clean_cbr_workspace_id=NULL` | `quarantine: false` |
| 14 à 16 | `destroy_tf_resources`, `update_db_for_resources`, `destroy_tf_workspace` (inchangés) | `action=destroy` | |
| 17 | `update_db_for_workspace` | `delete_status=success`, `status=terminated` | `delete_status: success` |

Ce qui change dans la validation (étape 1) :

- un bucket **plein n'est plus refusé** : c'est la suppression qui vide ;
  le refus « not empty » et le passage `LOCKED` associé disparaissent ;
- le **contrôle des verrous** du clean (ADR 0003, décision 6) est repris :
  un objet protégé par la politique du bucket libre après la fin de la
  grâce → refus avec la date à laquelle relancer ;
- une quarantaine résiduelle (clean échoué ou annulé, `quarantine_test`
  interrompu) est levée avant le listing, comme aujourd'hui ;
- une suppression déjà programmée (`delete_status` scheduled ou
  in_progress) → refus avec la date et l'action d'annulation ; un clean en
  cours → refus, comme aujourd'hui ;
- un bucket `terminated` → la demande finit **`SUCCESS` sans rien faire**
  (toutes les étapes court-circuitent). C'est la seule issue propre pour un
  `terraform destroy` relancé après un timeout du provider : la ressource
  sort du state Terraform au lieu d'échouer indéfiniment sur « already
  deleted ».

La programmation (étape 2) est une **mise à jour conditionnelle** (rowcount
1, `delete_status` ni scheduled ni in_progress, `clean_status` hors
`busy()`) : deux demandes delete validées en même temps n'en programment
qu'une, la seconde décline avant tout state et toute règle CBR.

### 2. La décision est datée et idempotente

Le claim (étape 5) est :

```sql
UPDATE bucket SET delete_status = 'in_progress'
WHERE subscription_id = :sub
  AND delete_execute_at = :execute_at
  AND delete_status IN ('scheduled', 'in_progress')
```

`IN` rend le claim idempotent sur un retry de tâche ; la condition sur la
date empêche un ancien run (suppression annulée, puis redemandée) de
réclamer la programmation d'un autre run. Zéro ligne → la demande est
déclinée : « cancelled during the grace period, nothing was destroyed », la
demande finit `CANCELLED`, le destroy Terraform échoue, la ressource reste
dans le state du client.

**Défaut trouvé dans le clean actuel au passage** : `transition_bucket_clean`
n'est conditionné qu'au statut, pas à la date. Clean programmé, annulé,
reprogrammé plus tard : le premier run se réveille à sa date initiale et
réclame la programmation du second. La même garde est à poser sur le claim
du clean, dans le même lot.

`delete_status = failed` n'est **jamais écrit avant le claim** : avant la
décision, la ligne reste `scheduled`, cohérente à la fois pour un retry de
la demande et pour une rétractation.

### 3. La rétractation est une action séparée : `cos.bucket.v1.cancel_delete`

Copie structurelle de `cancel_clean` (trois étapes) :

1. validation : `delete_status` scheduled avant `delete_execute_at`, ou
   `cancelled` (idempotent : ne refait que la levée), ou `failed` **tant que
   la règle d'expiration n'est pas posée** (`delete_rule_posed_at` nul,
   marqueur écrit par le DAG delete lui-même, sans dépendre de
   `has_expiration_rule` écrit hors dépôt) ; refusée dès la date, même si le
   claim n'a pas encore eu lieu, pour ne pas courir contre lui ;
2. `cancel_bucket_delete` : mise à jour conditionnelle `status=active`,
   `delete_status=cancelled`, `delete_execute_at=NULL` (le run suspendu ne
   peut plus matcher) ; rowcount 0 → « just started », le claim a gagné ;
3. levée de la quarantaine, `clean_cbr_workspace_id=NULL`, state
   `quarantine: false`.

Rien n'a été touché sur le bucket avant le claim : la rétractation restaure
le statut, la visibilité et l'accès réseau, sans rien réparer. Si la demande
suspendue peut être déclinée tout de suite par l'API du state manager
(`POST /state_manager/api/v1/demands/<uuid>/status`, endpoint déjà utilisé
par `cos-subscriptions/subscriptions_cleanup.py`), le destroy Terraform en
attente échoue en minutes et non à la date initiale : à conditionner à la
question 6 ci-dessous.

Un seul type d'action d'annulation côté provider et portail : soit
`cancel_delete` est déclaré sous le même type de ressource d'action que
`cancel_clean`, soit `cancel_clean` accepte aussi un bucket `terminating`.
Pas deux types tant que celui de `restore` n'est pas confirmé.

### 4. « Ne remonte plus » est porté par la base

Nouvelles colonnes de `bucket` : `delete_status` (scheduled, in_progress,
success, failed, cancelled ; `DeleteStatus.busy()` = scheduled,
in_progress), `delete_requested_at`, `delete_execute_at` (timestamptz),
`delete_requested_by`, `delete_rule_posed_at`. `clean_cbr_workspace_id`
garde son nom : c'est « le workspace de quarantaine du bucket », quel que
soit le DAG qui l'a posé. Les colonnes `clean_*` restent la propriété du
clean.

Deux vues créées par la migration :

```sql
CREATE VIEW bucket_visible AS
  SELECT * FROM bucket
  WHERE status IS DISTINCT FROM 'terminated'
    AND (delete_status IS NULL OR delete_status = 'cancelled');

CREATE VIEW bucket_pending_deletion AS
  SELECT subscription_id, name, delete_status, delete_requested_at,
         delete_execute_at, delete_requested_by, clean_cbr_workspace_id
  FROM bucket
  WHERE delete_status IN ('scheduled', 'in_progress', 'failed');
```

`bucket_visible` devient la lecture de référence des listings (portail,
services du projet d'équipe, `get_bucket_by_name` dans ce dépôt : on ne
restaure pas dans un bucket en grâce). `get_bucket_by_sub_id` reste sans
filtre : delete, cancel_delete et le sensor lisent la ligne masquée.
`bucket_pending_deletion` est la corbeille du portail et la surveillance
des grâces qui expirent et des règles CBR en cours.

### 5. Garde-fous d'exploitation

- **Plafond CBR** : une règle par bucket en grâce. À l'acceptation du
  delete, `COUNT(*)` des buckets du realm avec `clean_cbr_workspace_id` non
  nul contre un réglage `cos_quarantine_max_rules` → refus explicite
  « retry later » plutôt qu'un échec Schematics après la programmation.
  Chiffre à obtenir d'IBM avant une décommission massive.
- **Grâce à 0** (`cos_delete_grace_minutes = 0`) : aucune quarantaine
  posée, pose et levée en no-op. Pour la toolchain INT nocturne, qui finit
  chaque scénario par un destroy ; le chemin complet se valide par une
  campagne dédiée avec une grâce de cinq minutes.
- **Vidage borné en base** : dans le sensor quotidien, si `now >
  delete_execute_at + 7 jours`, `delete_status=failed` est écrit
  (quarantaine refermée au mieux) avant de lever ; la ligne ne reste jamais
  `in_progress` sans issue. Cause typique : legal hold ou rétention
  explicite plus longue que la politique (ADR 0003, décision 6).
- **Workspace du bucket disparu** (404 Schematics) : trancher avec
  `bucket_access_status` avant de compter le bucket détruit ; 200 ou 403 →
  `failed` + `LOCKED` « workspace gone, bucket still exists », au lieu de
  `terminated` sur un bucket orphelin (défaut existant, test
  `test_missing_workspace_counts_as_destroyed` à faire évoluer).
- **Update planifié déjà suspendu** quand le delete arrive : il a passé sa
  validation ; `update_tf_workspace` re-contrôle `delete_status` et
  `clean_status` au réveil de `wait_for_scheduled_time`.
- **Refus croisés** : update, restore, clean et `quarantine_test` refusent
  un bucket dont `delete_status` est busy, comme ils refusent déjà un clean
  busy. Dans restore, la comparaison à `RestoreStatus.RUNNING` est
  remplacée par `CleanStatus.busy()` + `DeleteStatus.busy()`.
- **Déploiement** : aucun run delete en vol quand la liste des tâches du
  delete change ; ne jamais renommer une étape de delete ou de clean avec
  des runs en grâce.
- `delete_notice` dit la date, l'action d'annulation, et que les
  sauvegardes du bucket échoueront pendant la grâce (ADR 0004).

## Ce qui a été écarté

- **Corbeille + purge planifiée** (demande delete courte, bucket en
  corbeille, DAG planifié hors demande qui purge, action `undelete`) : la
  plus robuste aux redémarrages, mais la ressource sort du state Terraform
  dès le `SUCCESS` ; un `apply` recrée un autre bucket pendant que les
  données sont en corbeille ; la rétractation dépend d'une demande sur une
  souscription terminée, que le dépôt contredit (`subscriptions_cleanup.py`
  traite un delete `SUCCESS` comme la fin de vie) ; et un second plan de
  contrôle hors demande dont les dépendances ne sont pas prouvées.
- **Clean puis delete enchaînés, bucket vide détruit sans grâce** : plus
  petit diff, mais contredit la demande (la grâce doit être
  inconditionnelle) ; « vide » mesuré sans versions ni delete markers ; et
  `cancel_clean` qui annule une suppression rend les messages et les états
  ambigus.
- **Grâce dans le delete en réutilisant les colonnes `clean_*`** : même
  architecture, mais trois colonnes à croiser pour savoir si c'est un clean
  ou une suppression ; colonnes `delete_*` dédiées retenues.
- **Rejoindre la demande en cours** (second destroy qui attend l'issue du
  premier) : un run observateur sans propriétaire clair, double les cas de
  panne ; à reconsidérer seulement si le timeout du provider l'impose.

## Avant de coder : lot 0, questions à l'équipe orchestrateur

Rien ne se code avant les réponses 1 et 2.

1. Une demande `cancel_delete` est-elle acceptée sur une souscription dont
   la demande delete est en cours (DAG suspendu dans un sensor différé,
   `status=terminating`) ? Le dépôt ne prouve la concurrence que pendant un
   clean, qui ne change pas le statut. Plan B : DAG d'exploitation hors
   `@product_action`, même mise à jour conditionnelle, même levée, sans
   state ; la rétractation n'est alors pas en libre-service.
2. Délai maximal d'attente du provider sur un Delete de
   `orchestrator_subscription_cosbucket_v1`. À l'expiration : la ressource
   reste-t-elle dans le tfstate, un destroy relancé crée-t-il une nouvelle
   demande ? Que fait-il d'une demande qui finit `CANCELLED` ? Le même
   problème existe déjà pour le clean et l'update planifié : à instruire une
   fois pour les trois.
3. Que devient la souscription quand la demande delete finit `SUCCESS` :
   supprimée, ou `TERMINATED` mais adressable ? Une demande delete sur une
   souscription terminée est-elle acceptée (no-op `SUCCESS`) ou refusée
   (404), et que fait le provider d'un 404 ?
4. Une demande peut-elle rester en cours quinze jours sans timeout côté API
   ni `dagrun_timeout` ? Aucun run de sept jours n'a encore tourné en INT.
5. Le retry d'une demande `ON_ERROR` rejoue-t-il une tâche d'un run vieux de
   dix jours, sensor différé compris ? C'est la sortie de la plupart des
   pannes après le claim.
6. Le DAG connaît-il l'uuid de sa propre demande, et peut-il la décliner via
   le state manager après une rétractation ? Le state poussé par une demande
   qui finit ensuite `CANCELLED` est-il conservé et relu par le client ?
7. Sémantique de `lock_subscription_on_failure` et du statut `LOCKED` côté
   orchestrateur : un delete en échec bloque-t-il `cancel_delete` ou le
   retry ?
8. Les lecteurs hors dépôt (portail, cosService, contextService,
   workspaceService, lifecyclePolicyRuleService, bucket_migration_service)
   lisent-ils la table `bucket` (bascule sur `bucket_visible`) ou l'API
   orchestrateur (c'est alors le statut orchestrateur qui cache le bucket) ?
9. Les actions `clean` et `cancel_clean` sont-elles exposées aujourd'hui par
   le provider et le portail, et sous quel type de ressource d'action ?
10. Plafond de règles CBR par compte IBM.
11. Persistance des triggers différés : à prouver une fois en INT avec une
    grâce de quinze minutes et un redémarrage volontaire du triggerer.

## Mise en œuvre, par lots déployables séparément

| Lot | Contenu | Ordre de grandeur |
|---|---|---|
| 0 | Questions ci-dessus, décision sur le no-op `SUCCESS` | 0,5 j |
| 1 | `DeleteStatus`, colonnes, deux vues, migration alembic, fonctions `bucketService` (programmation et annulation conditionnelles, claim daté), test d'intégration de la vue | 1,5 j |
| 2 | Extraction de `clean_service` (fonctions pures du vidage, les `@step` restent dans les DAGs, tests du clean inchangés), garde de date sur `claim_clean`, re-contrôle dans `update_tf_workspace` | 1,5 j |
| 3 | Nouveau delete, dix-sept étapes, tests ; grâce 0 ; garde CBR ; borne du sensor ; 404 workspace tranché par `bucket_access_status` | 3 j |
| 4 | `cancel_delete`, refus croisés (update, restore, clean, quarantine_test) | 1,5 j |
| 5 | README (actions, schéma du delete, diagramme d'états avec `TERMINATING -> ACTIVE`, vues, requête ops), README toolchain, réglages INT, scénario toolchain « bucket plein » piloté par Python, déroulé manuel de la rétractation, campagne INT grâce courte | 2 à 3 j |

Hors dépôt : lecteurs sur `bucket_visible`, portail (bouton d'annulation,
date, demande en cours), provider (rien d'obligatoire ; exposition de
`cancel_delete`), migration jouée.

Migration depuis `main` : les runs clean en cours continuent avec le même
code ; `delete_status` nul pour eux ; les `terminated` existants sortent de
la vue sans backfill ; les `locked` d'un ancien destroy échoué passent par
la grâce complète au prochain delete. Retour arrière : downgrade alembic et
ancien `delete.py` ; `cancel_delete` sur tout run en grâce avant de
revenir.

## Conséquences

- Une demande delete dure sept jours de grâce, un à sept jours de vidage,
  puis le destroy. Le client Terraform voit un destroy long ou expiré ; le
  client portail voit une demande en cours, une date, un bouton
  d'annulation. Le diagramme d'états du README gagne `TERMINATING -> ACTIVE`
  (rétractation).
- Un bucket plein n'est plus refusé au destroy : plus d'erreur « not
  empty ». C'était une rupture pour un `terraform destroy` qui devait vider
  à la main.
- Les clés de state sont additives (`delete_status`, `delete_requested_at`,
  `delete_execute_at`, `delete_notice`, `quarantine`) ; aucun champ de
  payload ajouté, donc aucun replace.
- La toolchain INT nocturne garde sa durée avec la grâce à 0 ; le chemin
  complet est une campagne à part.
- Points à surveiller : propagation CBR (quarantaine et levée en minutes),
  nombre de règles CBR, timeout du sensor quotidien, services internes
  (backup) bloqués pendant la grâce. Notification du demandeur à la
  programmation et la veille de l'exécution : toujours hors dépôt,
  `delete_requested_by` est prévu pour ça.
