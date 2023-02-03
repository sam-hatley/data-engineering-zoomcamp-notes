from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/sam-hatley/data-engineering-zoomcamp-notes.git"
)
block.get_directory("2_workflow_orchestration/homework")  # specify a subfolder of repo
block.save("dezoomcamp-git")

# prefect deployment build 2_workflow_orchestration/homework/etl_web_to_gcs_hw.py:etl_web_to_gcs --name etl_web_to_gcs --tag dezoomcamp-git -sb github/dezoomcamp-git -a
