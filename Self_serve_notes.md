# Some cool tricks in vscode

Edit multiple lines: `alt` + `shift` + `I`
Edit multiple occurances: `ctrl` + `shift` + `L`

# Introduction to Docker

```
docker run -it --entrypoint=bash python:3.9
```
Creates a python 3.9 env that starts you in bash, so that we can use pip to install software.

Docker images *always start in the state that you initalized them in*, so you will need to create a dockerfile to create your image. Something like the above would look like:
```dockerfile
FROM python:3.9

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```

To execute it from the directory you're starting docker in: `docker build -t test:pandas ./2_DOCKER_SQL`, in this particular case. You can then run it from `docker run -it test:pandas`.

To run it with a file:
```dockerfile
FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
```

## Ingesting NY Taxi Data to Postgres

Selecting an internal file with the -v flag allows us to keep a local copy of the database.

```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/sam/git/data-engineering-zoomcamp-notes/2_DOCKER_SQL/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

To see what's running on docker, run `docker ps`. To connect to the psql database:
```
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

Used `wget` with [these datasets](https://github.com/DataTalksClub/nyc-tlc-data) to import the data, then used a [jupyter notebook](/2_DOCKER_SQL/upload-data.ipynb) to process and upload the data into the database. Will ultimately use the jupyter notebook to test the code, then copy it over to a script for ingestion.

# Connecting pgAdmin and Postgres

Can just run a docker container with pgAdmin already installed:

```bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

We need to place the psql instance and the pgAdmin instance on the same network for pgAdmin to access it.

Create a network:
```
docker network create pg-network
```

Now run our instances within that network:

```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/sam/git/data-engineering-zoomcamp-notes/2_DOCKER_SQL/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin\
    dpage/pgadmin4
```

Once we've connected to `http://localhost:8080/` with the credentials admin@admin.com:root, we can connect using the name `pg-database`: **not** localhost.

# Dockerizing the Ingestion script

This includes turning the jupyter notebook into the [ingest_data.py](/2_DOCKER_SQL/ingest_data.py) program.

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python3 ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}
```

Next, we'll update the dockerfile to include dependencies:

```dockerfile
FROM python:3.9

RUN apt install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
```

build the file:
```
docker build -t taxi_ingest:v001 .
```

run the file:
```
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}
```

If you want to start a local server with python:
```bash
python3 -m http.server
```
so we can gzip the file then link to `http://172.24.57.193:8000/output.csv.gz`, or whatever ip debian throws with the command `ip addr`.

Stopped on [1.2.5](https://www.youtube.com/watch?v=hKI6PkPhpa0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=10)

## Running psql and pgAdmin with docker-compose

Instead of passing the above commands repeatedly, will package them into a single `YAML` file to create and start all relevant services. We'll build the file as [docker-compose.yaml](/2_DOCKER_SQL/docker-compose.yaml). We don't need to specify a network here, as they're being defined together.

Once the yaml is finished, run `docker-compose up`, which will run the file. `docker-compose up -d` runs it in detached mode, which preserves the terminal.
To kill it, run `docker-compose down`.

# 1.2.6 SQL refresher

Let's upload the zone-refresh data. We'll use a [jupyter notebook](/2_DOCKER_SQL/ingest_zones.ipynb) to do so.