
import boto3
from sagemaker.inputs import TrainingInput
from datetime import datetime
import sagemaker
from sagemaker import image_uris
from sagemaker import Session
from sagemaker.workflow.pipeline_context import PipelineSession

# Define your S3 bucket and prefix
bucket = 'diabetes'
prefix = 'pipeline'
region = 'ap-south-1'
input_source = f"s3://{bucket}/datasets/diabetes.csv"
train_path = f"s3://{bucket}/{prefix}/train"
test_path = f"s3://{bucket}/{prefix}/test"
val_path = f"s3://{bucket}/{prefix}/val"

# Generate a timestamp
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")  # Format: YYYYMMDD-HHMMSS
#model_output_uri
model_output_uri = f"s3://{bucket}/{prefix}/models/{timestamp}/output"

train_input = TrainingInput(s3_data=train_path, content_type='text/csv')
evaluation_output_uri = f"s3://{bucket}/evaluation/report"

######instances-require for the pipeline###

processing_image_uri = image_uris.retrieve(framework='sklearn', region=region, version='1.0-1')
training_image_uri = image_uris.retrieve(framework='sklearn', region=region, version='1.0-1')
evaluation_image_uri = image_uris.retrieve(framework='sklearn', region=region, version='1.0-1')
instance_count = 1
instance_type = 'ml.m5.xlarge'


################# important for pipeline #####################
sagemaker_session = sagemaker.Session()
role = 'arn:aws:iam::590183717898:role/service-role/AmazonSageMaker-ExecutionRole-20240716T105741'  #sagemaker.get_execution_role()
pipeline_session = PipelineSession()

import sagemaker
from sagemaker.processing import (
    ProcessingInput,
    ProcessingOutput,
    ScriptProcessor,  # Use ScriptProcessor if you are using a script
)
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline

# Create a ScriptProcessor
sklearn_processor = ScriptProcessor(
    image_uri=processing_image_uri,
    role=role,
    instance_type=instance_type,
    instance_count=instance_count,
    base_job_name='diabetes-new'
)

# Define processing step
processing_step = ProcessingStep(
    name='PreprocessingStep',
    processor=sklearn_processor,
    code='preprocess.py',  # Path to your preprocessing script
    inputs=[
        ProcessingInput(
            source=input_source,
            destination="/opt/ml/processing/input",
            s3_input_mode="File",
            s3_data_distribution_type="ShardedByS3Key",
        )
    ],
    outputs=[
        ProcessingOutput(
            output_name="train_data",
            source="/opt/ml/processing/output/train",
            destination=train_path,
            s3_upload_mode="EndOfJob",
        ),
        ProcessingOutput(
            output_name="test_data",
            source="/opt/ml/processing/output/test",
            destination=test_path,
            s3_upload_mode="EndOfJob",
        ),
        ProcessingOutput(
            output_name="val_data",
            source="/opt/ml/processing/output/validation",
            destination=val_path,
            s3_upload_mode="EndOfJob",
        ),
    ]
)

# Create the pipeline
pipeline = Pipeline(
    name='DiabetesProcessingPipeline',
    steps=[processing_step]
)

# Execute the pipeline
pipeline.create(role_arn=role)  # Creates the pipeline
pipeline.start()  # Starts the pipeline execution


while True:
    status = pipeline_execution.describe()['PipelineExecutionStatus']
    print(f"Pipeline status: {status}")
    if status in ['Succeeded', 'Failed', 'Stopped']:
        break
    time.sleep(30)  # Check the status every 30 seconds

print("Pipeline execution completed.")
