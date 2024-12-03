import google.cloud.aiplatform as aip

from google_cloud_pipeline_components.v1.custom_job import utils
from kfp import compiler, dsl


@dsl.component
def custom_train_model(user_input: str):
    print(user_input)


@dsl.pipeline
def pipeline():
    custom_job_distributed_training_op = utils.create_custom_training_job_from_component(
        custom_train_model,
        replica_count=1,
        machine_type="n1-standard-4",
        #accelerator_type=TRAIN_GPU.name,
        #accelerator_count=TRAIN_NGPU,
        boot_disk_type="pd-ssd",
        boot_disk_size_gb=100
    )
    custom_producer_task = custom_job_distributed_training_op(user_input="Hello World!")


def main():
    job_name = 'custom_training_pipeline'
    job_spec_file_name = f'{job_name}.yaml'

    # Then we compile our pipeline
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=job_spec_file_name)

    # Finally we create and submit our pipeline job
    job = aip.PipelineJob(
        display_name=job_name,
        template_path=job_spec_file_name,
        parameter_values={})

    job.submit()


if __name__ == "__main__":
    main()
