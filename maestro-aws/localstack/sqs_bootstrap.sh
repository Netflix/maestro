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

# Dead-letter queue for maestro-event messages that exceed max receive count
create_queue "maestro-event-dlq"

# Queue for maestro-extensions to consume maestro events (subscribed to SNS topic)
# Redrive policy sends messages to DLQ after 5 failed processing attempts
create_queue "maestro-event"
DLQ_ARN="arn:aws:sqs:${AWS_REGION}:000000000000:maestro-event-dlq"
awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs set-queue-attributes \
    --queue-url http://${LOCALSTACK_HOST}:4566/000000000000/maestro-event \
    --region ${AWS_REGION} \
    --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"
