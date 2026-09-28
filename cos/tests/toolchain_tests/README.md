# Tests Terraform de la toolchain

Les dossiers `int/`, `qual/`, `pprod/`, `prod/` contiennent les `.tf` qui créent des
COS, des buckets et des backup vaults via le provider `orchestrator`
(`source = "bp2i/orchestrator"`, téléchargé depuis `repo.artifactory-dogen.group.echonet`).

## Préparer l'environnement : `toolchain_env.py`

Avant, il fallait à la main : `terraform login`, `terraform init`, se connecter à l'UI
Vault de l'environnement, copier un token, le coller dans un `curl` pour lire l'API key
IBM Cloud, puis l'exporter dans `IBM_CLOUD_API_KEY` et `ORCHESTRATOR_IBMCLOUD_API_KEY`.
Le script enchaîne tout cela et ne refait que ce qui est nécessaire :

| Étape | Sautée si… |
|---|---|
| `terraform login <artifactory>` | un credential est déjà dans `~/.terraform.d/credentials.tfrc.json` (ou `TF_TOKEN_…`) |
| `terraform init` | `.terraform/` existe déjà dans le dossier (`--reinit` pour forcer) |
| token Vault via Chrome / Edge en navigation privée (login SSO) | le token sauvegardé est encore accepté (`lookup-self`), ou `$VAULT_TOKEN` / `--vault-token` |
| `GET <vault>/v1/ibm_<compte>/creds/<rôle>_buhub` | l'API key sauvegardée a encore un lease valide |
| export des variables | jamais |

Le cache est dans `~/.cache/cos-toolchain/state.json` (lisible par soi seul).

```bash
cd tests/toolchain_tests

# tout enchaîner puis terraform plan / apply dans int/new_version (ou int/)
python toolchain_env.py --env int --run plan
python toolchain_env.py --env int --run apply -- -auto-approve

# exporter les variables dans le shell courant, puis utiliser terraform normalement
eval "$(python toolchain_env.py --env int)"
terraform -chdir=int/new_version plan

# un sous-shell avec les variables déjà exportées
python toolchain_env.py --env int --shell

# menus numérotés (défaut sans argument dans un terminal)
python toolchain_env.py
```

Options utiles : `--dir` (autre dossier terraform), `--tf-log` (`TF_LOG=debug`),
`--proxy` (`http_proxy` / `https_proxy` = `ncproxy:8080` + `no_proxy`), `--new-token`,
`--new-key`, `--forget`, `--manual-token` (copier-coller depuis l'UI Vault au lieu de
piloter Chrome), `--vault` / `--vault-url` / `--secret-path` / `--namespace` pour
sortir des valeurs par défaut. `--help` liste tout.

### Environnement → Vault → secret

| `--env` | Vault | secret lu |
|---|---|---|
| `int` | `hvault-dev.fr.net.intra` | `ibm_ac002i000263/creds/rl002i000138_buhub` |
| `qual`, `pprod` | `hvault.staging.echonet` | `ibm_ac002i000263/creds/rl002i000077_buhub` (à vérifier) |
| `prod` | `hvault.group.echonet` | `ibm_ac002i000266/creds/rl002i000102_buhub` |

Namespace Vault : `AP85135`. Tout est dans `ENVIRONMENTS` / `VAULTS` en tête du script.

### Comment le token Vault est lu dans Chrome

Même mécanisme que `cos-subscriptions/subscriptions_cleanup.py` : Chrome / Edge est lancé
avec un profil temporaire, en navigation privée, avec le protocole DevTools activé sur un
port local. Le script ouvre l'UI Vault, attend le login SSO, puis évalue dans la page une
expression JS qui cherche le token de session dans `sessionStorage` / `localStorage`.
Dès qu'un token est trouvé, le navigateur est fermé et le profil supprimé. Sans Chrome,
ou avec `--manual-token`, l'UI s'ouvre dans le navigateur par défaut : "Copy token" dans
le menu utilisateur, puis Entrée dans le terminal.

## Tests du script

```bash
python -m pytest tests/toolchain_tests        # depuis cos/
```
