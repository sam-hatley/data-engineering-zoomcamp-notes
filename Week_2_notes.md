# Introduction to Prefect Concepts

Using anaconda, useful concepts:

- Creating an env: `conda create -n {env name}python=3.9`
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

Run it with `pip install -r requirements.txt`