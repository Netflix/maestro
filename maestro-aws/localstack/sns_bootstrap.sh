#!/usr/bin/env bash

set -euo pipefail

# enable debug
# set -x

echo "configuring sns"
echo "==================="
LOCALSTACK_HOST=localhost
AWS_REGION=us-east-1

awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sns create-topic --name maestro-test --region ${AWS_REGION}
