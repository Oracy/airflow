# Airflow Docker

Clone this repository, and after that pull submodules with:

```bash
git submodule init && git submodule update
```

To initiate airflow db, run command:

```bash
docker-compose up airflow-init
```

To start airflow webserver
```bash
docker-compose up -d
```

to stop airflow
```bash
docker-compose down
```

To login, access http://localhost:8080 with:

```
user: airflow
password: airflow
```

To access database and create your source to use on flows localhost:5432

```
user: airflow
password: airflow
```
