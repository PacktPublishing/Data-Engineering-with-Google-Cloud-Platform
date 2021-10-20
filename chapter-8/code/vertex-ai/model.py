import os
import sys

from google.cloud import aiplatform
from google.cloud.aiplatform import gapic as aip

PROJECT_ID = "aw-general-dev"
REGION= "us-central1"
BUCKET_NAME = "aw-general-dev-mlengine"

aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET_NAME)


TRAIN_GPU, TRAIN_NGPU = (None, None)
DEPLOY_GPU, DEPLOY_NGPU = (None, None)



TRAIN_IMAGE = "us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest"
DEPLOY_IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/scikit-learn-cpu.0-23:latest"

print("Training:", TRAIN_IMAGE, TRAIN_GPU, TRAIN_NGPU)
print("Deployment:", DEPLOY_IMAGE, DEPLOY_GPU, DEPLOY_NGPU)


MACHINE_TYPE = "n1-standard"

VCPU = "4"
TRAIN_COMPUTE = MACHINE_TYPE + "-" + VCPU
print("Train machine type", TRAIN_COMPUTE)

MACHINE_TYPE = "n1-standard"

VCPU = "4"
DEPLOY_COMPUTE = MACHINE_TYPE + "-" + VCPU
print("Deploy machine type", DEPLOY_COMPUTE)


JOB_NAME = "custom_job_1"
MODEL_DIR = "{}/{}".format(BUCKET_NAME, JOB_NAME)
TRAIN_STRATEGY = "single"



job = aiplatform.CustomTrainingJob(
    display_name=JOB_NAME,
    script_path="ai-pipeline-credit-default-train.py",
    container_uri=TRAIN_IMAGE,
    requirements=["google-cloud-bigquery","google-cloud-storage","pandas","pyarrow"],
    model_serving_container_image_uri=DEPLOY_IMAGE,
)

MODEL_DISPLAY_NAME = "model_cc"

model = job.run(
        model_display_name=MODEL_DISPLAY_NAME,
        replica_count=1,
        machine_type=TRAIN_COMPUTE,
        accelerator_count=0,
    )