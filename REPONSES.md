# Réponses TP 0 à TP 3 (Apache Airflow 3.0.0)

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
