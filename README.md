Maestro
===================================
[Maestro](https://netflixtechblog.com/orchestrating-data-ml-workflows-at-scale-with-netflix-maestro-aaa2b41b800c)
is a general-purpose workflow orchestrator that 
provides a fully managed workflow-as-a-service (WAAS) to the data platform users at Netflix.

It serves thousands of users, including data scientists, data engineers, machine learning engineers,
software engineers, content producers, and business analysts, for various use cases.
It schedules hundreds of thousands of workflows, millions of jobs every day
and operates with a strict SLO even when there are spikes in the traffic.
Maestro is highly scalable and extensible to support existing and new use cases and offers enhanced usability to end users.

You can read more details about it in our latest [blog post](https://netflixtechblog.com/maestro-netflixs-workflow-orchestrator-ee13a06f9c78).

# Get started
## Prerequisite
- Git
- Java 21
- Gradle
- Docker


## Build it
- `./gradlew build`

## Run it
- `./gradlew bootRun`

## Run it with AWS module
- `docker compose -f maestro-aws/docker-compose.yml up`
- `./gradlew bootRun --args='--spring.profiles.active=aws'`

## Create a sample workflow
- `curl --header "user: tester" -X POST 'http://127.0.0.1:8080/api/v3/workflows' -H "Content-Type: application/json" -d @maestro-server/src/test/resources/samples/sample-dag-test-1.json`

## Get the sample workflow definition
- `curl -X GET 'http://127.0.0.1:8080/api/v3/workflows/sample-dag-test-1/versions/latest'`

## Trigger to run the sample workflow
- `curl --header "user: tester" -X POST 'http://127.0.0.1:8080/api/v3/workflows/sample-dag-test-1/versions/latest/actions/start' -H "Content-Type: application/json" -d '{"initiator": {"type": "manual"}}'`

## Get the sample workflow instance
- `curl -X GET 'http://127.0.0.1:8080/api/v3/workflows/sample-dag-test-1/instances/1/runs/1'`

## Delete the sample workflow and its data
- `curl --header "user: tester" -X DELETE 'http://127.0.0.1:8080/api/v3/workflows/sample-dag-test-1'`

# License
Copyright 2024 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
