# Introduction to Prefect Concepts

Using anaconda, useful concepts:

- Creating an env: `conda create -n {env name} python=3.9`
- Using the env: `conda activate {env name}`
- Going back to base: `conda deactivate`

requirements.txt simplifies the install:
```
    pandas==1.5.2
    prefect==2.7.7
    prefect-sqlalchemy==0.2.2
    prefect-gcp[cloud_storage]==0.2.3
    protobuf==4.21.11
    pyarrow==10.0.1
    pandas-gbq==0.18.1
    psycopg2-binary==2.9.5
    sqlalchemy==1.4.46
```

Install these packages with `pip install -r requirements.txt`

Build a prefect deployment with the below commands:
```bash
prefect deployment build ./$LOCATION.py:$PARENT_FUNCITON -n $NAME --cron "* * * * *" -a
```
The `-a` above automatically applies the deployment. Can also do the below, as an example:
```bash
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
prefect deployment apply etl_parent_flow-deployment.yaml
```

To build and push an image to docker:
```bash
docker image build -t hatleys/prefect-test:v01 .
docker image push hatleys/prefect-test:v01
```
Make sure not to forget the `.` in the build stage.
The block building process can be automated with a [python file](docker_deployment/make_docker_block.py), as can the [deployment](docker_deployment/docker_deploy.py).

I gave up on connecting the two after a while- no matter what I tried, I would receive the following error:

```
/usr/local/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
14:06:19.341 | ERROR   | prefect.engine - Engine execution of flow run '50e4574e-b27b-4651-8b28-8877bbd1059a' exited with unexpected exception
...
ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 4200)
...
httpx.ConnectError: All connection attempts failed
14:06:19.586 | WARNING | prefect.infrastructure.docker-container - Docker container keen-axolotl was removed before we could wait for its completion.
```