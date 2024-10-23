import boto3
from sagemaker.inputs import TrainingInput
from datetime import datetime
import sagemaker
from sagemaker import image_uris
from sagemaker import Session
from sagemaker.workflow.pipeline_context import PipelineSession

# Define your S3 bucket and prefix
bucket = 'diabates'
prefix = 'pipeline'
region = 'ap-south-1'
input_source = f"s3://{bucket}/datasets/diabetes.csv"
train_path = f"s3://{bucket}/{prefix}/train"
test_path = f"s3://{bucket}/{prefix}/test"
val_path = f"s3://{bucket}/{prefix}/val"
training_model_file = 'model.tar.gz'

# Generate a timestamp
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")  # Format: YYYYMMDD-HHMMSS
#model_output_uri
model_output_uri = f"s3://{bucket}/{prefix}/pipeline_run_version_{timestamp}"

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
#########training step########

from sagemaker.estimator import Estimator
from sagemaker.inputs import TrainingInput
from sagemaker.workflow.steps import TrainingStep
from sagemaker.workflow.pipeline import Pipeline
from datetime import datetime

# Define the input data for training
train_input = TrainingInput(s3_data=train_path, content_type='text/csv')

# Define the Estimator
estimator = Estimator(
    entry_point='train.py',  # Your training script
    image_uri=training_image_uri,
    py_version='py3',
    instance_type=instance_type,  # Specify the instance type
    instance_count=1,  # Set the instance count
    role=role,
    output_path=model_output_uri,  # This should include the timestamp
    base_job_name='sklearn-diabetes',  # Update base job name if needed
    hyperparameters={
        'n_estimators': 50,
        'max_depth': 5
    }
)

# Define the training step
train_step = TrainingStep(
    name="TrainModel",
    estimator=estimator,
    inputs={
        "training": train_input,
    },
)

# Create the pipeline
pipeline = Pipeline(
    name='DiabetestrainingPipeline-4',
    steps=[train_step]  # Include the training step
)

# Create and start the pipeline
#pipeline.create(role_arn=role)  # Creates the pipeline
pipeline.start()  # Starts the pipeline execution

