# Tests Terraform de la toolchain

Les dossiers `int/`, `qual/`, `pprod/`, `prod/` contiennent les `.tf` qui créent des
COS, des buckets et des backup vaults via le provider `orchestrator`
(`source = "bp2i/orchestrator"`, téléchargé depuis `repo.artifactory-dogen.group.echonet`).

## Préparer l'environnement : `toolchain_env.py`

Avant, il fallait à la main : `terraform login`, `terraform init`, exporter le proxy avec
son user et son mot de passe, appeler le service token (`GET /v1/token/<uid>`) dans son
Swagger, se connecter à l'UI Vault avec le `client_token` obtenu, le recopier depuis son
profil, le coller dans un `curl` pour lire l'API key IBM Cloud, puis exporter
`IBM_CLOUD_API_KEY` et `ORCHESTRATOR_IBMCLOUD_API_KEY`. Le script enchaîne tout cela et
ne refait que ce qui est nécessaire :

| Étape | Sautée si… |
|---|---|
| `terraform login <artifactory>` | un credential est déjà dans `~/.terraform.d/credentials.tfrc.json` (ou `TF_TOKEN_…`) |
| `terraform init` | `.terraform/` existe déjà dans le dossier (`--reinit` pour forcer) |
| proxy `http://<user>:<mdp>@ncproxy.fr.net.intra:8080` (user et mot de passe demandés) | `https_proxy` est déjà exporté dans le shell, ou `--no-proxy` |
| token Vault : `GET <service token>/v1/token/<uid>?namespace=AP85135` → `auth.client_token` | le token sauvegardé est encore accepté (`lookup-self`), ou `$VAULT_TOKEN` / `--vault-token` |
| `GET <vault>/v1/ibm_<compte>/creds/<rôle>_buhub` | l'API key sauvegardée a encore un lease valide |
| export des variables | jamais |

Le `client_token` renvoyé par le service token est directement un token Vault (30 jours) :
c'est celui que l'UI affiche ensuite dans « Copy token ». Le script l'utilise donc tel
quel, sans passer par l'UI. L'uid (ex. `la90261`) est demandé au premier lancement puis
mémorisé, comme le user du proxy ; le mot de passe du proxy est redemandé à chaque
lancement et n'est jamais écrit sur disque. Le cache est dans
`~/.cache/cos-toolchain/state.json` (lisible par soi seul).

```bash
cd tests/toolchain_tests

# tout enchaîner puis terraform plan / apply dans int/new_version (ou int/)
python toolchain_env.py --env int --run plan
python toolchain_env.py --env int --run apply -- -auto-approve

# exporter les variables (API key + proxy) dans le shell courant, puis terraform normalement
eval "$(python toolchain_env.py --env int)"
terraform -chdir=int/new_version plan

# un sous-shell avec les variables déjà exportées
python toolchain_env.py --env int --shell

# menus numérotés (défaut sans argument dans un terminal)
python toolchain_env.py
```

Options utiles : `--uid`, `--proxy-user` / `--proxy-password` (ou `$PROXY_USER` /
`$PROXY_PASSWORD`) pour ne rien saisir, `--no-proxy`, `--dir` (autre dossier terraform),
`--tf-log` (`TF_LOG=debug`), `--new-token`, `--new-key`, `--forget`, `--browser-token`
(Chrome sur l'UI Vault au lieu du service token), `--manual-token` (copier-coller depuis
l'UI Vault), `--vault` / `--vault-url` / `--token-service` / `--secret-path` /
`--namespace` pour sortir des valeurs par défaut. `--help` liste tout.

### Environnement → Vault → secret

| `--env` | Vault | service token | secret lu |
|---|---|---|---|
| `int` | `hvault-dev.fr.net.intra` | `https://s02vi9956141:4430` | `ibm_ac002i000263/creds/rl002i000138_buhub` |
| `qual`, `pprod` | `hvault.staging.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000263/creds/rl002i000077_buhub` (à vérifier) |
| `prod` | `hvault.group.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000266/creds/rl002i000102_buhub` |

Namespace Vault : `AP85135`. Tout est dans `ENVIRONMENTS`, `VAULTS` et `TOKEN_SERVICES`
en tête du script. `no_proxy` vaut `localhost,127.0.0.1,.echonet,0.0.0.0` : les Vault
`.echonet` sont joints en direct, `hvault-dev.fr.net.intra` et le service token via le proxy.

### Secours : token lu dans Chrome

Sans service token pour l'instance Vault (ou avec `--browser-token`), même mécanisme que
`cos-subscriptions/subscriptions_cleanup.py` : Chrome / Edge est lancé avec un profil
temporaire, en navigation privée, avec le protocole DevTools activé sur un port local. Le
script ouvre l'UI Vault, attend le login, puis évalue dans la page une expression JS qui
cherche le token de session dans `sessionStorage` / `localStorage`. Dès qu'un token est
trouvé, le navigateur est fermé et le profil supprimé. Sans Chrome, ou avec
`--manual-token`, l'UI s'ouvre dans le navigateur par défaut : « Copy token » dans le
menu utilisateur, puis Entrée dans le terminal.

## Tests du script

```bash
python -m pytest tests/toolchain_tests        # depuis cos/
```
