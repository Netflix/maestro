# Maestro AWS module
This module includes maestro interface implementations using AWS services (i.e. SNS and SQS).

To use it in maestro-server example, please use the aws profile to start the spring boot server.
Please update the application-aws.yml with your AWS credentials and configurations for sns and sqs.

If testing locally, please use the localstack to start the sns and sqs services by

```bash
cd maestro-aws
docker-compose up
```
