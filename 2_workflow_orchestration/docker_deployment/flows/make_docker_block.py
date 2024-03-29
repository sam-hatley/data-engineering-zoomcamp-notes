# This creates a docker block within a running Prefect server

from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image="hatleys/prefect:v03",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("prefect-test", overwrite=True)

# python ./make_docker_block.py
# This creates the BLOCK- not the deployment.
