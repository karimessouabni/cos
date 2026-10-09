# SchematicsGitlabTokenError intermittent sur update de workspace Schematics (« Gitlab token invalid »)

> Destinataire : équipe BP2I Terraform.
> Les identifiants internes sont masqués dans ce fichier (dépôt public). Les valeurs
> réelles sont à reporter depuis les logs Airflow avant envoi : champs `<...>`.
> Les champs entre crochets `[...]` sont à compléter.

## Résumé

Échec récurrent de la task [nom de la task] du DAG [nom du DAG] lors de l'update d'un
workspace Schematics : le workspace passe en `FAILED` avec « Gitlab token invalid ».
Un simple re-run de la task passe sans aucune autre action.

- Fréquence : [x fois / semaine]
- Environnement : [prod / preprod]

## Occurrence de référence

| Champ | Valeur |
|---|---|
| Date | 2026-09-29 15:12:42 → 15:12:53 UTC |
| Subscription | `<SUBSCRIPTION_ID>` |
| Demand | `<DEMAND_ID>` |
| Workspace | `eu-de.workspace.ws_cos_<SUBSCRIPTION_ID>…<SUFFIXE>` |
| Worker Airflow | `airflow-worker-4` (namespace `<NAMESPACE>`) |
| Agent Schematics | `<AGENT_NAME>` |
| requestID | `<REQUEST_ID>` |
| Orchestrator | `<ORCHESTRATOR_ID>` |

## Erreur

```
bp2i_terraform.backends.schematics.SchematicsGitlabTokenError:
Workspace eu-de.workspace.ws_cos_<...> FAILED - Gitlab token invalid.
```

Stack :

- `bp2i_terraform/backends/schematics.py`, l.779, `update()`
- `bp2i_terraform/backends/schematics.py`, l.504, `_wait_for_workspace()`

Dernier log de l'agent avant l'échec : `Ready to execute the command on Agent <AGENT_NAME>`.

## Impact

Relance manuelle nécessaire à chaque occurrence, demandes COS retardées, bruit côté
support.

## Analyse

Le token est valide au re-run : ce n'est donc pas un problème de configuration ni de
droits permanents. Le token semble devenir invalide entre son obtention et le clone du
repo par l'agent Schematics (étape asynchrone).

Hypothèses, par ordre de vraisemblance (non vérifiées, faute d'accès au code de
`bp2i_terraform`) :

1. **Course sur la rotation du token.** Si chaque run fait un `rotate` (ou revoke +
   create) sur le même access token GitLab, l'ancien est révoqué immédiatement. Deux
   demandes en parallèle : A obtient T1, B rotate et obtient T2, l'agent de A clone avec
   T1 et échoue.
   Test : les échecs coïncident-ils avec des runs concurrents ?
2. **Token expiré avant consommation.** Le token est récupéré en début de task (ou dans
   une task amont via XCom), puis le job attend dans la file de l'agent. Si le TTL est
   court, il expire avant le `git clone`.
   Test : comparer l'heure d'obtention du token et l'heure du « Ready to execute the
   command on Agent ».
3. **Token pas encore propagé côté GitLab.** Un token créé juste avant le PATCH du
   workspace peut être refusé quelques secondes (réplicas en lecture, Geo).
   Test : l'échec arrive-t-il quelques secondes seulement après la création ?
4. **Faux positif de classification.** La l.504 de `_wait_for_workspace` mappe peut-être
   tout échec de clone (timeout, 429, 5xx, ban après échecs d'auth répétés) vers
   `SchematicsGitlabTokenError`.
   Test : lire le log du job Schematics (activity ID) et les logs d'auth GitLab à
   15:12:42 UTC pour voir le vrai code HTTP.

## Questions à l'équipe

1. Comment le token GitLab est-il obtenu (rotation, création par run, Vault) et quel est
   son TTL ?
2. Des runs concurrents peuvent-ils s'invalider mutuellement (rotate qui révoque le
   token précédent) ?
3. La l.504 classe-t-elle tout échec de clone (timeout, 429, 5xx) en
   `SchematicsGitlabTokenError` ? Quel est le code HTTP réel renvoyé par GitLab ?

## Demande

- Identifier la cause racine.
- En attendant : retry dans `update()` sur `SchematicsGitlabTokenError`, avec
  récupération d'un token frais et nouveau PATCH du workspace (2-3 tentatives avec
  backoff).

## Pistes de correction selon la cause

| Cause | Correction |
|---|---|
| 1. Rotation concurrente | Ne plus faire de rotation par run : token dédié longue durée (deploy token ou project access token en `read_repository`) stocké dans Vault, renouvelé hors bande avec chevauchement. À défaut, sérialiser via un pool Airflow à 1 slot. |
| 2. Expiration | Récupérer le token juste avant le PATCH, dans la même task, et allonger le TTL au-delà du pire délai de file de l'agent. |
| 3. Propagation | Valider le token avant de l'envoyer à Schematics (`GET /api/v4/personal_access_tokens/self` ou `git ls-remote`), avec backoff jusqu'au 200. |
| 4. Mauvaise classification | Affiner le parsing pour distinguer un 401 d'un timeout ou d'un 429. |

## Autres occurrences

| Date (UTC) | Demand | Run concurrent sur le même repo ? |
|---|---|---|
| [à compléter] | `<DEMAND_ID>` | [oui / non] |
