Maestro
===================================
[Maestro](https://netflixtechblog.com/orchestrating-data-ml-workflows-at-scale-with-netflix-maestro-aaa2b41b800c#7d0f)
is a general-purpose workflow orchestrator that 
provides a fully managed workflow-as-a-service (WAAS) to the data platform at Netflix.

It serves thousands of users, including data scientists, data engineers, machine learning engineers,
software engineers, content producers, and business analysts, for various use cases.
It schedules hundreds of thousands of workflows, millions of jobs every day
and operate with a strict SLO of less than 1 minute of scheduler introduced delay
even when there are spikes in the traffic.
Maestro is highly scalable and extensible to support existing and new use cases and offers enhanced usability to end users.

# Get started
## Prerequisite
- Git
- Java 8
- Gradle
- Docker


## Build it
- `./gradlew build`

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
