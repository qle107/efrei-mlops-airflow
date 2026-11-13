# Réponses TP 0 à TP 3 (Apache Airflow 3.0.0)

Binôme : Quang Dat LE, Adrien CHUEMBOU MBAH

Projet : `airflow-intro/` (docker-compose.yaml, .env, dags/, logs/, plugins/, config/, .venv)

## TP 0 : Environnement

| Élément | Résultat |
|---|---|
| Poste | 31 Go RAM, 16 threads, 332 Go libres sur C: |
| Docker | Docker 28.4.0, Compose v2.39.4, `docker run hello-world` → « Hello from Docker! » |
| `.wslconfig` | `[wsl2] memory=8GB processors=4` (appliqué au prochain `wsl --shutdown`) |
| uv | 0.12.9 |
| VS Code | 1.128.0, extensions Python + Docker (Microsoft) installées |
| PowerShell | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` → RemoteSigned |

## TP 1 : Installer et lancer Airflow (checklist)

- [x] Dossier `airflow-intro` avec docker-compose.yaml, .env (`AIRFLOW_UID=50000`), dags/, logs/, plugins/, config/
- [x] `.venv` en Python 3.12 (`uv venv --python 3.12`), sélectionné dans VS Code (`.vscode/settings.json`)
- [x] `apache-airflow==3.0.0` installé avec les contraintes 3.0.0 / py3.12 (même version que l'image)
- [x] 5 services healthy : airflow-apiserver, airflow-dag-processor, airflow-scheduler, airflow-triggerer, postgres. airflow-init : Exited (0)
- [x] Connexion OK sur http://localhost:8080 (airflow / airflow)
- Conflit port 8080 : un autre conteneur Airflow (`docker-airflow-1`) a été arrêté avec `docker stop`.

## TP 2 : Découvrir l'interface

États obtenus pour le premier run de demo_ui : extraire = success, transformer = failed, charger = upstream_failed. Pour demo_ok : dire_bonjour = success.

Q1. Avec seulement les deux DAGs de démo, il y a 2 Dag Runs : 1 success (demo_ok) et 1 failed (demo_ui). (Sur mon instance, l'historique contenait aussi les runs de user_processing : 8 runs, 7 success et 1 failed.)

Q2. Avec les deux démos, il y a 4 task instances : 2 success (extraire, dire_bonjour), 1 failed (transformer) et 1 upstream_failed (charger).

Q3. Active Dags = 2 (3 avec user_processing), Running Dags = 0. Un DAG actif est simplement non mis en pause. Un DAG running est en cours d'exécution à cet instant. Les runs de démo durent quelques secondes, donc au moment de la lecture aucun ne tourne.

Q4. La santé du scheduler (bloc Health). S'il est rouge, plus rien n'est planifié.

Q5. Le filtre d'état de la vue DAGs porte sur l'état du dernier run du DAG, pas sur chaque tâche. Ce run est failed à cause de transformer, même si extraire a réussi.

Q6. Après correction (`> 100`), le nouveau run est success, avec extraire, transformer et charger tous en success.

Q7. Observé : v1. Avec Airflow 3.0.0 et le bundle local `dags-folder`, qui n'est pas versionné, une nouvelle version n'est créée que si la structure sérialisée du DAG change (tâches, dépendances, paramètres). Ici, seul le corps de `transformer` a changé. Le nouveau run exécute bien le nouveau code et reste en v1. Pour obtenir une v2, il faut ajouter ou renommer une tâche, ou changer un paramètre du DAG.

Q8. Clear remet la tâche à zéro et la réexécute (le try number augmente). Mark as success force l'état sans rien exécuter, donc le problème n'est pas corrigé et les tâches en aval tournent sans les données attendues.

Q9. Un XCom, car c'est une valeur produite par une tâche et lue par la suivante. Une Variable sert à partager une valeur de configuration entre plusieurs DAGs.

Q10. Installer le provider `apache-airflow-providers-amazon` dans l'image ou les conteneurs Airflow (image personnalisée, ou `_PIP_ADDITIONAL_REQUIREMENTS` pour un test), puis redémarrer. (Note : dans l'image 3.0.0 officielle, `amazon` fait partie des 29 providers installés.)

Checklist : demo_ui et demo_ok exécutés. Les 3 états ont été repérés. demo_ok a été mis en pause puis réactivé. demo_ui a été corrigé puis relancé avec succès. Le XCom d'extraire vaut 42. La variable api_endpoint a été créée puis supprimée. Les vues Assets et Admin ont été localisées.

## TP 3 : Mon premier pipeline (`dags/user_processing.py`)

Résultat : 5 tâches vertes. 3 runs successifs sont success, ce qui valide le bonus idempotence. `SELECT * FROM users;` renvoie `4815162 | Camille | Martin | camille.martin@example.com | 2026-11-13 09:14:40`.

### Partie 0
- airflow-init Exited : ce n'est pas un problème. Ce conteneur ponctuel migre la base, crée le compte admin et prépare les dossiers, puis s'arrête avec le code 0.
- Healthy : MetaDatabase, Scheduler, Triggerer et DAG Processor, soit la base PostgreSQL, le scheduler, le triggerer et le DAG processor du tableau. L'API server n'y figure pas puisque c'est lui qui sert la page.

### Partie 1
- Sans l'appel final `user_processing()`, le DAG n'est jamais créé et n'apparaît pas, sans aucune erreur.
- Le décorateur `dag` est dans le module `airflow.sdk` (Task SDK d'Airflow 3).

### Partie 2
- Host `postgres` : chaque conteneur a son propre localhost. `postgres` est le nom du service dans docker-compose.yaml, résolu par le DNS du réseau Docker.
- 5432 est le port par défaut de PostgreSQL. Celui de MySQL est 3306.
- Sans `IF NOT EXISTS`, le deuxième run échoue avec `relation "users" already exists`.
- Elasticsearch absent : le provider correspondant n'est pas installé. Il faut installer `apache-airflow-providers-elasticsearch` dans les conteneurs Airflow.

### Partie 3
- `airflow tasks test <dag_id> <task_id>`, par exemple `airflow tasks test user_processing create_table`.
- `airflow tasks list user_processing` liste les tâches.
- Pour vérifier la table : `airflow db shell` puis `\dt users` ou `SELECT * FROM users;`. Équivalent depuis l'hôte : `docker compose exec postgres psql -U airflow -c "\dt"`.

### Partie 4
- 200 = OK. Autres codes : 404 Not Found (ressource absente), 500 Internal Server Error (erreur serveur), et aussi 401 Unauthorized, 503 Service Unavailable.
- `is_done` indique si la condition est remplie (True arrête le sensor, False le fait réessayer après poke_interval). `xcom_value` est la valeur poussée en XCom quand le sensor réussit, ici le JSON de l'utilisateur.
- `import requests` est placé dans la fonction parce que le DAG processor relit le fichier en permanence. Les imports en haut du fichier ralentissent ce parsing. Dans la fonction, l'import n'a lieu qu'à l'exécution de la tâche.
- HttpSensor vérifie seulement que l'endpoint répond, via une connexion HTTP, et ne renvoie pas le contenu. Notre `@task.sensor` renvoie les données grâce à `xcom_value`.

### Partie 5
- `ti` = TaskInstance, injecté par Airflow selon le nom du paramètre (contexte). Si on le renomme, il n'est plus injecté.
- Pour voir un XCom dans l'interface : run → tâche → onglet XCom, ou Browse → XComs.
- Les clés sont sensibles à la casse : `firstname` lève `KeyError`, il faut `firstName`.

### Partie 6
- Ordre des colonnes : COPY remplit les colonnes par position, pas par nom. Constaté en vrai : le dictionnaire reçu via XCom n'avait plus ses clés dans l'ordre (`id,email,lastname,firstname`), et l'email s'est retrouvé dans `firstname`. Correction : `fieldnames` explicites dans l'ordre de la table.
- /tmp : c'est le /tmp du conteneur qui exécute la tâche. Avec le LocalExecutor, c'est le conteneur scheduler, pas le PC.
- En production avec plusieurs workers, process_user et store_user peuvent tourner sur des machines différentes, et le fichier n'existe pas là où il est lu. Il faut un stockage partagé (S3, GCS, MinIO, volume partagé) ou charger directement en base.

### Partie 7
- Le `.venv` ne sert qu'à l'autocomplétion dans VS Code. Le code s'exécute dans les conteneurs Docker, donc le provider doit être présent dans l'image (il y est déjà dans l'image officielle).
- `HEADER` : la première ligne du CSV est l'en-tête, elle est ignorée.
- La table a 5 colonnes. Le CSV en avait 4 avant l'ajout de `created_at`.

### Partie 8
- `create_table` doit précéder `is_api_available()` pour que `>>` s'applique à la première tâche de la chaîne. Ainsi, la table existe avant tout le reste. Écrit plus loin, la dépendance se placerait au mauvais endroit.
- Tous les jours à 8 h : `@dag(schedule="0 8 * * *", start_date=..., catchup=False)`. C'est 8 h UTC, soit 9 h ou 10 h à Paris.
- Second lancement sans le bonus : store_user échoue avec `duplicate key value violates unique constraint "users_pkey"`, car l'API renvoie toujours le même id.

### Bonus : idempotence
Choix : piste 2. COPY vers une table temporaire, puis `INSERT ... ON CONFLICT (id) DO NOTHING`, dans une seule transaction.
- TRUNCATE perd l'historique. La piste 2 le conserve et convient à une vraie API qui renvoie de nouveaux utilisateurs.
- Pour mettre à jour l'utilisateur : `ON CONFLICT (id) DO UPDATE SET firstname = EXCLUDED.firstname, lastname = EXCLUDED.lastname, email = EXCLUDED.email, created_at = EXCLUDED.created_at`.
- `create_table` est idempotente grâce à `IF NOT EXISTS`.

### Bilan
1. Un opérateur est un modèle de tâche (une classe). Une tâche est une instance d'opérateur dans un DAG, identifiée par son task_id. Un DAG est le graphe de tâches et de dépendances, sans cycle, avec sa planification.
2. Les identifiants sont stockés dans une connexion Airflow (en base, chiffrée) et le code ne contient que le `conn_id`. On évite ainsi les secrets dans Git, et on peut changer d'environnement sans toucher au code.
3. Non. Un sensor est une tâche du DAG, exécutée par un worker (ici le scheduler, avec le LocalExecutor), et il occupe un slot pendant son attente.
4. `@task` supprime le task_id et le python_callable, et gère les XComs automatiquement (le retour d'une tâche passé en argument fait le push et le pull). Il est plus lisible. Inconvénients : tout ce qui est retourné part en XCom, la sérialisation peut perdre l'ordre des clés (cas vécu en partie 6), et l'accès au contexte (`ti`) est moins direct.
5. Les XComs sont stockés dans la base de métadonnées (table xcom). Un fichier de 2 Go saturerait la base et la mémoire. Il faut stocker le fichier ailleurs (S3, disque partagé) et ne passer que son chemin.
6. Un hook gère la connexion et expose des méthodes utilisables en Python. Un opérateur est une tâche prête à l'emploi qui utilise un hook en interne. Le hook est indispensable pour `copy_expert` (COPY en masse), que SQLExecuteQueryOperator n'expose pas.
7. `fake_user >> user_info` est déjà implicite, car extract_user reçoit le résultat du sensor. create_table n'échange aucune donnée avec le sensor, donc la dépendance doit être explicite.
8. Deux points à revoir : (a) remplacer le fichier `/tmp` par un stockage partagé, car avec plusieurs workers les tâches ne voient pas le même disque. (b) Mettre les secrets dans un secrets backend ou Vault, ajouter des retries et des alertes, et fixer une planification avec catchup. L'idempotence (bonus) est déjà en place.

## TP CI/CD : GitHub Actions pour Airflow

Dépôt : https://github.com/qle107/efrei-mlops-airflow (déjà créé avant ce TP, sous ce nom plutôt que `airflow-intro`). Workflow : `.github/workflows/ci.yml`, jobs `lint` puis `test-dags` (Airflow 3.0.0, version de l'image dans docker-compose.yaml). Tests : `tests/test_dags.py`.

### Partie 1
- `git status` montre avant le commit ce qui va partir (fichiers suivis, modifiés, non suivis). On évite ainsi d'envoyer un secret ou des fichiers lourds par erreur.
- Sans `.gitignore`, on aurait envoyé `.venv/` (des milliers de fichiers), `logs/` (logs générés par Airflow), `__pycache__/`/`*.pyc` et surtout `.env`, avec ses variables locales, qui ne doit jamais partir sur GitHub.
- Vérification : sur GitHub on voit `dags/user_processing.py` et `docker-compose.yaml`, mais pas de `logs/`.

### Partie 2 (run « Premier workflow CI », vert en 7 s)
Le log de « Dire bonjour » affiche `Bonjour depuis le runner GitHub`, puis `demo_ok.py`, `demo_ui.py` et `user_processing.py`.
- Sans `actions/checkout@v4`, le runner est vide : `ls dags/` échoue (`No such file or directory`) et le job passe au rouge, puisque le runner n'a pas notre code.
- Une indentation YAML fausse change la structure (une clé se retrouve rattachée au mauvais parent) ou rend le fichier invalide. GitHub affiche alors « Invalid workflow file » et ne lance rien. Comme en Python, l'indentation fait partie de la syntaxe.

### Partie 3
- En local, `uvx flake8 dags/ --select E9,F63,F7,F82 --show-source` n'affiche rien, et la commande de style (max 120) n'affiche aucune remarque. autopep8 n'a donc rien eu à corriger.
- Job `lint` vert sur GitHub.
- F401 : module importé mais jamais utilisé. F821 : nom non défini (variable ou fonction utilisée sans être définie ni importée).

### Partie 4
- Version : `apache/airflow:3.0.0`, d'où `AIRFLOW_VERSION: "3.0.0"`. `from airflow.models import DagBag` fonctionne en 3.0.0.
- Test complété : `assert taches == attendues`.
- Vérification : les deux jobs sont verts et le log affiche `3 passed`.
- `needs: lint` : test-dags ne démarre qu'après un lint vert. Si le lint échoue, test-dags est marqué **skipped**, comme observé au cas A. On ne paie pas 1 à 3 minutes d'installation d'Airflow pour un code qui ne compile pas.
- Constraints : le fichier fige la version de chacune des centaines de dépendances d'Airflow, avec un ensemble testé par le projet. Sans lui, pip prend les dernières versions, qui peuvent être incompatibles. Le build casse alors du jour au lendemain sans que notre code ait changé, et il n'est plus reproductible.
- Provider Postgres : le runner part de zéro et n'a rien à voir avec notre image Docker. `pip install apache-airflow` n'installe pas `apache-airflow-providers-postgres`, donc l'import de `PostgresHook` lèverait `ModuleNotFoundError` dans le DagBag.

### Partie 5 (runs réels)

| Cas | Modification | Job qui échoue | Message à retrouver |
|---|---|---|---|
| A | Suppression de `from ...postgres import PostgresHook` | `lint` (test-dags : skipped) | `dags/user_processing.py:63:16: F821 undefined name 'PostgresHook'` |
| B | Suppression de l'appel final `user_processing()` | `test-dags` (lint vert) | `AssertionError: assert 'user_processing' in {'demo_ok': ..., 'demo_ui': ...}` puis `KeyError: 'user_processing'`, soit 2 failed, 1 passed |
| C | `store_user` renommée `save_user` | `test-dags` (lint vert) | `AssertionError: Tâches différentes : {'store_user', 'save_user'}`, soit 1 failed, 2 passed |

Après chaque annulation, le commit est de nouveau vert (« Annulation cas B », « Annulation cas C »). Note : mes deux premiers essais de B et C (commits 7fdedbd, cfb79f7) cumulaient encore le cas A, faute d'annulation entre les deux. Ils ont été refaits séparément.
- Cas B, flake8 ne voit rien parce que le fichier reste du Python valide : la fonction `user_processing` est définie et simplement jamais appelée. Ce n'est ni une erreur de syntaxe ni un nom non défini. Seul le chargement réel (DagBag) montre qu'aucun DAG n'est créé.
- Cas C, l'utilité vient du test, qui sert de contrat : d'autres éléments dépendent du task_id (historique des runs, XComs tirés par task_id, alertes, dashboards, DAGs aval). Un renommage silencieux casse ces liens. Le test oblige à le rendre volontaire.
- Cas B dans l'interface : aucune erreur ne s'affiche, le DAG disparaît simplement de la liste après le rescan du DAG processor, qui peut prendre plusieurs minutes. On peut chercher longtemps puisque « DAG Import Errors » reste vide. La CI le signale en environ 1 minute avec un message explicite.

### Pour aller plus loin : PR
- Le binôme ne doit pas valider sa propre PR. Avec la protection de branche, l'auteur ne peut pas approuver sa propre PR : il faut un autre relecteur. La relecture humaine voit ce que la CI ne teste pas : la logique métier, la lisibilité, les secrets en clair, la bonne planification, les effets de bord en production.

### Bilan
1. Rencontrées pendant les TP Airflow : (a) le DAG absent quand l'appel final `user_processing()` manque, sans aucun message, que `test_user_processing_existe` attrape (cas B) ; (b) un provider ou un import manquant. Par exemple, sans le provider Postgres, le DAG apparaît dans « DAG Import Errors » seulement après le rescan, alors que la CI l'attrape au push (cas A, test d'import). Elle n'aurait pas attrapé les erreurs d'exécution (KeyError `firstname`, ordre des colonnes du CSV), qui demandent des tests unitaires des tâches.
2. Le lint est rapide (environ 10 s) et attrape les erreurs grossières. Les tests demandent d'installer Airflow, soit plusieurs minutes. Échouer vite et pas cher d'abord évite de gaspiller du temps de runner, et le premier message lu est le plus simple.
3. En MLOps, la CI peut aussi vérifier : le schéma et la qualité des données (colonnes, types, valeurs absurdes comme des prix négatifs), le ré-entraînement sur un petit jeu, des métriques minimales (R², MAE en dessous d'un seuil, pas de régression par rapport au modèle en production), le chargement et une prédiction de référence du modèle sérialisé, ainsi que le versionnage du modèle (MLflow).
