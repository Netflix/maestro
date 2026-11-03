/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.extensions.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Reference to a specific step instance attempt.
 *
 * @param workflowId the workflow id
 * @param workflowInstanceId the workflow instance id
 * @param workflowRunId the workflow run id
 * @param stepId the step id
 * @param stepAttemptId the step attempt id
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id"
    },
    alphabetic = true)
public record StepInstanceReference(
    String workflowId,
    long workflowInstanceId,
    long workflowRunId,
    String stepId,
    long stepAttemptId) {}
