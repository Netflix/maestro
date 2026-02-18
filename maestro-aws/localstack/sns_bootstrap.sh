#!/usr/bin/env bash

set -euo pipefail

# enable debug
# set -x

echo "configuring sns"
echo "==================="
LOCALSTACK_HOST=localhost
AWS_REGION=us-east-1

awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sns create-topic --name maestro-test --region ${AWS_REGION}

# Subscribe maestro-event SQS queue to SNS topic for maestro-extensions consumption
# RawMessageDelivery=true sends the raw message body (not the SNS envelope JSON),
# matching internal's raw delivery configuration.
SUBSCRIPTION_ARN=$(awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sns subscribe \
    --topic-arn arn:aws:sns:${AWS_REGION}:000000000000:maestro-test \
    --protocol sqs \
    --notification-endpoint arn:aws:sqs:${AWS_REGION}:000000000000:maestro-event \
    --region ${AWS_REGION} \
    --output text --query 'SubscriptionArn')

awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sns set-subscription-attributes \
    --subscription-arn "${SUBSCRIPTION_ARN}" \
    --attribute-name RawMessageDelivery \
    --attribute-value true \
    --region ${AWS_REGION}
