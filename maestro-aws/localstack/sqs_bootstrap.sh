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

awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs create-queue --queue-name maestro-time-trigger-execution --region ${AWS_REGION} --attributes VisibilityTimeout=120

create_fifo_queue "maestro-signal-instance.fifo"
create_fifo_queue "maestro-signal-trigger-match.fifo"
create_fifo_queue "maestro-signal-trigger-execution.fifo"
