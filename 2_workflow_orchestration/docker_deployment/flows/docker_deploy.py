# This creates a deployment from a flow with a docker container

from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import etl_parent_flow

docker_block = DockerContainer.load("prefect-test")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow, name="docker-flow", infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()

# prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
