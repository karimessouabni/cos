# Installer le projet `cos` sur un poste macOS (sans droits admin)

Guide pas à pas pour obtenir un environnement local complet : Python compatible,
librairies internes `bp2i-airflow-library` et `bp2i-terraform` installées depuis
Artifactory, PyCharm qui résout les imports.

Toutes les commandes se lancent depuis la racine du projet
(`~/PycharmProjects/cos`), sauf mention contraire. Remplacez `<login>` par votre
identifiant BNP (matricule) et `<token>` par le jeton concerné.

---

## 0. Ce qu'il faut savoir avant de commencer

| Sujet | Ce que dit le projet |
|---|---|
| Version Python | `pyproject.toml` : `python = ">=3.10,<3.13 \|\| >3.14,<3.15"`. Airflow 2.11.1 ne supporte que 3.9 à 3.12 : **prendre 3.12** (ou 3.11). |
| Librairies internes | `bp2i-airflow-library = "2.34.4"`, `bp2i-terraform = "2.6.20-rc.0"`, déclarées dans `pyproject.toml`, servies par Artifactory. Pas besoin de les cloner pour les installer. |
| Sources Poetry | `artifactory-pypi` (miroir PyPI) et `ap43584-pypi-local` (paquets internes), toutes deux en `priority = "primary"` : **tout** passe par Artifactory, même les paquets publics. |
| Réseau | L'URL Artifactory est en `.net.intra` : réseau BNP ou VPN obligatoire. Le serveur redirige `http` vers `https` avec un certificat signé par la CA interne. |
| Python système | `/usr/bin/python3` est en 3.9.6 : inutilisable pour ce projet (syntaxe `X \| Y` dans le code). |

---

## 1. Obtenir un Python 3.12

### Option A : Homebrew (pas besoin d'admin)

```bash
brew install python@3.12
ls /opt/homebrew/opt/python@3.12/bin/python3.12
```

**Erreur rencontrée** : `Error: undefined method '[]' for nil` dans
`Utils::Bottles.load_tab` pendant le "pouring" de `ca-certificates`. Le bottle
téléchargé est incomplet (proxy d'entreprise). Remède :

```bash
brew update
brew config | grep -iE "BOTTLE_DOMAIN|ARTIFACT_DOMAIN|PROXY"   # un miroir interne ?
rm -rf "$(brew --cache)/downloads/"*ca-certificates*
brew install --build-from-source ca-certificates
brew install python@3.12
```

Si `openssl@3` échoue de la même façon : `brew install --build-from-source openssl@3`.

### Option B : uv (Python autonome dans le home)

`uv` s'installe comme un paquet Python via Artifactory, puis télécharge un Python
depuis GitHub (peut être bloqué par le proxy).

```bash
/usr/bin/python3 -m pip install --user uv \
  --index-url http://repo.artifactory-dogen.group.echonet.net.intra/artifactory/api/pypi/pypi/simple \
  --trusted-host repo.artifactory-dogen.group.echonet.net.intra
~/.local/bin/uv python install 3.12
~/.local/bin/uv python find 3.12        # chemin à donner à poetry env use
```

### Ce qui ne marche PAS sans admin

- L'installeur `.pkg` de python.org (demande un mot de passe administrateur).
- Le téléchargement de Python depuis PyCharm (il lance ce même `.pkg`).

### Repli

Le catalogue Self Service (Jamf) du poste, ou un ticket "Python 3.12 pour
développement".

---

## 2. Accès GitLab en ligne de commande (SSO)

Le SSO ne sert que pour le navigateur. Pour `git clone` il faut un jeton ou une
clé SSH.

**Jeton d'accès personnel** : GitLab > avatar > Edit profile > Access Tokens,
scope `read_repository`. Puis :

```bash
git config --global credential.helper osxkeychain
git clone https://gitlab-dogen.group.echonet/<groupe>/<projet>.git
# Username : <login>   Password : <token GitLab>
```

**Clé SSH** (si le port 22 passe) :

```bash
ssh-keygen -t ed25519 -C "gitlab-bnp"
cat ~/.ssh/id_ed25519.pub     # à coller dans GitLab > Edit profile > SSH Keys
```

---

## 3. Réseau, proxy et certificat TLS

### 3.1 Vérifier que Artifactory est joignable

```bash
nslookup repo.artifactory-dogen.group.echonet.net.intra
curl -sS -m 10 -o /dev/null -w "%{http_code}\n" \
  http://repo.artifactory-dogen.group.echonet.net.intra/artifactory/api/pypi/pypi/simple/
```

| Résultat | Signification |
|---|---|
| `NXDOMAIN` / timeout | Pas sur le réseau BNP : VPN. |
| `302` | Le serveur répond et redirige vers `https`. Normal. |
| `401` | Réseau OK, il manque les identifiants Artifactory (section 4). |

### 3.2 Exclure les hôtes internes du proxy

Si `env | grep -i proxy` montre `HTTP_PROXY` / `HTTPS_PROXY`, Poetry envoie les
requêtes `.intra` au proxy Internet, qui échoue. À mettre dans `~/.zshrc` :

```bash
export NO_PROXY=".intra,.echonet,localhost,127.0.0.1"
export no_proxy="$NO_PROXY"
```

### 3.3 Faire confiance à la CA interne

**Erreurs rencontrées** :

- `Could not find a suitable TLS CA certificate bundle, invalid path: /opt/homebrew/opt/certifi/.../cacert.pem`
  → une variable `REQUESTS_CA_BUNDLE` ou `SSL_CERT_FILE` pointe vers un fichier
  disparu. La retrouver : `grep -n certifi ~/.zshrc ~/.zprofile`.
- `SSLCertVerificationError: self-signed certificate in certificate chain`
  → Python ne connaît pas la CA de la banque (curl, lui, lit le trousseau macOS).

Remède : construire un bundle à partir du trousseau et le déclarer partout.

```bash
security find-certificate -a -p /Library/Keychains/System.keychain > ~/bnp-ca-bundle.pem
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain >> ~/bnp-ca-bundle.pem
# si la CA est dans le trousseau de session :
security find-certificate -a -p ~/Library/Keychains/login.keychain-db >> ~/bnp-ca-bundle.pem
grep -c "BEGIN CERTIFICATE" ~/bnp-ca-bundle.pem      # > 100 attendu

# vérification : 200 ou 401 attendu, pas d'erreur SSL
curl -sS --cacert ~/bnp-ca-bundle.pem -o /dev/null -w "%{http_code}\n" \
  https://repo.artifactory-dogen.group.echonet.net.intra/artifactory/api/pypi/pypi/simple/
```

Dans `~/.zshrc` (remplace toute ancienne ligne `certifi`) :

```bash
export REQUESTS_CA_BUNDLE="$HOME/bnp-ca-bundle.pem"
export SSL_CERT_FILE="$HOME/bnp-ca-bundle.pem"
```

Puis `source ~/.zshrc`. Et pour Poetry, source par source :

```bash
poetry config certificates.artifactory-pypi.cert ~/bnp-ca-bundle.pem
poetry config certificates.ap43584-pypi-local.cert ~/bnp-ca-bundle.pem
```

> À éviter : `poetry config certificates.<source>.cert false` désactive la
> vérification TLS. Ça débloque, mais ce n'est pas acceptable sur un poste
> d'entreprise.

---

## 4. Identifiants Artifactory

Artifactory refuse le mot de passe SSO en ligne de commande : il faut un jeton.

1. Ouvrir `http://repo.artifactory-dogen.group.echonet.net.intra` dans le navigateur (SSO).
2. Nom en haut à droite > **Edit Profile** > **Generate an Identity Token**
   (ou **Generate API Key** selon la version). Copier le jeton immédiatement.
3. Tester puis configurer Poetry pour les **deux** sources :

```bash
curl -sSL -u "<login>:<token>" -o /dev/null -w "%{http_code}\n" \
  http://repo.artifactory-dogen.group.echonet.net.intra/artifactory/api/pypi/pypi/simple/
# 200 attendu

poetry config http-basic.artifactory-pypi <login> "<token>"
poetry config http-basic.ap43584-pypi-local <login> "<token>"
```

Poetry range les identifiants dans le trousseau macOS ou dans
`~/Library/Application Support/pypoetry/auth.toml`, jamais dans le projet.

Optionnel, pour `pip` seul (`~/.config/pip/pip.conf`, droits `600`, jamais versionné) :

```ini
[global]
index-url = http://<login>:<token>@repo.artifactory-dogen.group.echonet.net.intra/artifactory/api/pypi/pypi/simple
trusted-host = repo.artifactory-dogen.group.echonet.net.intra
```

---

## 5. Créer le venv et installer

```bash
deactivate 2>/dev/null                      # si un ancien .venv est activé dans le shell
cd ~/PycharmProjects/cos
poetry env remove --all
poetry env use /opt/homebrew/opt/python@3.12/bin/python3.12   # ou le chemin renvoyé par uv
poetry run python --version                 # 3.12.x attendu
poetry install
poetry run pip show bp2i-airflow-library bp2i-terraform
poetry run python -c "from bp2i_airflow_library.dag import product_action, step; from bp2i_terraform.backends.schematics import TerraformVar; print('ok')"
```

**Pièges rencontrés** :

- `poetry env use Python@3.12` → "Could not find the python executable" :
  `Python@3.12` est un nom de formule Homebrew, pas un binaire. Donner le chemin
  complet ou `python3.12` s'il est dans le PATH.
- "Current Python version (3.9.6) is not allowed by the project" : le venv a été
  supprimé et Poetry retombe sur le Python système. Refaire `poetry env use`.
- Le prompt affiche encore `(.venv)` après `poetry env remove` : reste du shell,
  faire `deactivate`.
- Le nom du paquet est `bp2i-airflow-library` (tirets) pour pip et Poetry,
  `bp2i_airflow_library` (underscores) à l'import Python.

Pour diagnostiquer un échec de `poetry install` :

```bash
poetry install -vvv 2>&1 | grep -iE "ssl|certificate|proxy|Max retries|Errno|401|403" | head
```

---

## 6. Configurer PyCharm

1. **Settings > Project: cos > Python Interpreter** : sélectionner
   `~/PycharmProjects/cos/.venv/bin/python` (re-sélectionner même si le chemin
   n'a pas changé, le venv a été recréé).
2. Clic droit sur le dossier `cos/` (celui qui contient `cos_service/`) >
   **Mark Directory as > Sources Root**, pour résoudre les imports `cos_service.*`.
   `add_project_to_path()` en tête des DAGs ne fait ce travail qu'à l'exécution.
3. **File > Invalidate Caches > Invalidate and Restart**.

Les imports `bp2i_airflow_library` et `bp2i_terraform` passent au vert et
Cmd+clic navigue dans la lib installée.

---

## 7. Tests et typage

### Tests sans les vraies libs

`tests/conftest.py` installe les doublures de `tests/stubs/` dans `sys.modules`
quand les libs bp2i ne sont pas importables. Les tests tournent donc même sans
Artifactory, mais un test vert sur les stubs ne prouve pas la compatibilité avec
la lib : la CI reste le juge.

```bash
poetry run python -m pytest tests -q
```

### mypy

Les erreurs "X | Y syntax for unions requires Python 3.10" viennent d'un
`python_version` trop ancien dans la config mypy (`pyproject.toml` ou `mypy.ini`).
L'aligner sur `3.12` :

```bash
grep -n python_version pyproject.toml mypy.ini setup.cfg 2>/dev/null
poetry run mypy cos_service
```

---

## 8. Récapitulatif des erreurs et de leur cause

| Message | Cause | Section |
|---|---|---|
| `Package(s) not found: bp2i_airflow_library` | Lib non installée dans le venv (Python 3.14, ou `poetry install` en échec) | 1, 5 |
| `undefined method '[]' for nil` (brew) | Bottle `ca-certificates` incomplet, proxy | 1 |
| `Could not find the python executable Python@3.12` | Mauvaise syntaxe `poetry env use` | 5 |
| `Current Python version (3.9.6) is not allowed` | Venv supprimé, Python système pris par défaut | 5 |
| `All attempts to connect to repo.artifactory... failed` | Proxy, ou TLS (voir les lignes suivantes) | 3 |
| `Could not find a suitable TLS CA certificate bundle` | `REQUESTS_CA_BUNDLE` pointe vers un fichier absent | 3.3 |
| `CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain` | CA interne inconnue de Python | 3.3 |
| `401` sur Artifactory | Identifiants manquants ou faux | 4 |
| Installeur Python demande un mot de passe admin | `.pkg` python.org, y compris via PyCharm | 1 |
