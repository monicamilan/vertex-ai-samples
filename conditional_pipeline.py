import google.cloud.aiplatform as aip

from google_cloud_pipeline_components.v1.custom_job import utils
from kfp import compiler, dsl
from kfp.dsl import Input, Dataset, OutputPath
from typing import NamedTuple

@dsl.component(
    packages_to_install=["pandas"]
)
def proces_files(type: str, input_data: Input[Dataset]):
    import pandas as pd
    input_files = pd.read_csv(input_data.path)[type].tolist()
    for file in input_files:
        print(f"Processing this file: {file}")
@dsl.component(
    base_image="python:3.9",
    packages_to_install=["pandas"]
)
def orchestrator(
        # inputs
        input_files: list,
        # outputs
        pdfs_list_path: OutputPath("Dataset"),
        tiffs_list_path: OutputPath("Dataset"),
) -> NamedTuple("Outputs", [("pdf", str), ("tiff", str)]):
    import pandas as pd

    pdfs = []
    tiffs = []

    for file in input_files:
        if file.endswith(".pdf"):
            pdfs.append(file)
        elif file.endswith(".tiff"):
            tiffs.append(file)

    pdfs_df = pd.DataFrame({"pdf": pdfs})
    pdfs_df.to_csv(pdfs_list_path, index=False)

    tiffs_df = pd.DataFrame({"tiff": tiffs})
    tiffs_df.to_csv(tiffs_list_path, index=False)

    pdf = "false" if not pdfs else "true"
    tiff = "false" if not tiffs else "true"
    return (pdf, tiff)


@dsl.pipeline
def pipeline(input_files: list):
    task1_op = orchestrator(input_files=input_files)
    custom_job_distributed_training_op = utils.create_custom_training_job_from_component(
        proces_files,
        replica_count=1,
        machine_type="n1-standard-4",
        # accelerator_type=TRAIN_GPU.name,
        # accelerator_count=TRAIN_NGPU,
        boot_disk_type="pd-ssd",
        boot_disk_size_gb=100
    )
    with dsl.If(task1_op.outputs["pdf"]=="true"):
        custom_job_distributed_training_op(type="pdf", input_data=task1_op.outputs['pdfs_list_path'])
    with dsl.If(task1_op.outputs["tiff"]=="true"):
        custom_job_distributed_training_op(type="tiff", input_data=task1_op.outputs['tiffs_list_path'])


def main():
    job_name = 'conditional_pipeline'
    job_spec_file_name = f'{job_name}.yaml'

    # Then we compile our pipeline
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=job_spec_file_name)

    # Finally we create and submit our pipeline job
    job = aip.PipelineJob(
        display_name=job_name,
        template_path=job_spec_file_name,
        parameter_values={"input_files": ["test.pdf", "test2.tiff", "test3.pdf", "test4.tiff"]}
    )
    job.submit()


if __name__ == "__main__":
    main()

