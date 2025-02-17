#!/usr/bin/env bash

set -euo pipefail

# enable debug
# set -x

echo "configuring sqs"
echo "==================="
LOCALSTACK_HOST=localhost
AWS_REGION=us-east-1

create_queue() {
    local QUEUE_NAME_TO_CREATE=$1
    awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs create-queue --queue-name ${QUEUE_NAME_TO_CREATE} --region ${AWS_REGION} --attributes VisibilityTimeout=30
}

create_fifo_queue() {
    local QUEUE_NAME_TO_CREATE=$1
    awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs create-queue --queue-name ${QUEUE_NAME_TO_CREATE} --region ${AWS_REGION}  --attributes FifoQueue=true
}

create_queue "maestro-start-workflow-job"
create_fifo_queue "maestro-run-workflow-instances-job.fifo"
create_queue "maestro-terminate-instances-job"
create_queue "maestro-terminate-then-run-instance-job"
create_queue "maestro-publish-job-event"
create_queue "maestro-delete-workflow-job"
create_fifo_queue "maestro-step-wake-up-job.fifo"
