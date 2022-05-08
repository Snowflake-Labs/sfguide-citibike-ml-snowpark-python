#!/bin/sh

REGION=<aws-region>
ACCOUNT_ID=<aws-account-id>
IMAGE_NAME=conda-env-kernel
ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/RoleName

aws --region ${REGION} ecr create-repository --repository-name smstudio-custom
    
aws --region ${REGION} ecr get-login-password | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/smstudio-custom

docker build . -t ${IMAGE_NAME} -t ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/smstudio-custom:${IMAGE_NAME}

docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/smstudio-custom:${IMAGE_NAME}

aws --region ${REGION} sagemaker create-image --image-name ${IMAGE_NAME} --role-arn ${ROLE_ARN}

aws --region ${REGION} sagemaker create-image-version --image-name ${IMAGE_NAME} \
    --base-image "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/smstudio-custom:${IMAGE_NAME}"

aws --region ${REGION} sagemaker describe-image-version --image-name ${IMAGE_NAME}

aws --region ${REGION} sagemaker create-app-image-config --cli-input-json file://app-image-config-input.json

#If you don't have a Sagemaker domain already...
#aws --region ${REGION} sagemaker create-domain --cli-input-json file://create-domain-input.json

#!!!! udpate the file update-domain-input.json with your sagemaker domain id !!!!

aws --region ${REGION} sagemaker update-domain --cli-input-json file://update-domain-input.json
