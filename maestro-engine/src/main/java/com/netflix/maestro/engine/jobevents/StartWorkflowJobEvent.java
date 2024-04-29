/*
 * Copyright 2024 Netflix, Inc.
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
package com.netflix.maestro.engine.jobevents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Maestro internal job event to inform run strategy to try to start workflow instances for the
 * given workflow id. It is sent after creating queued workflow instances. It can be used to start a
 * single workflow instance as well.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id"},
    alphabetic = true)
@Data
public class StartWorkflowJobEvent implements MaestroJobEvent {
  @Valid @NotNull private String workflowId;

  /** static creator. */
  public static StartWorkflowJobEvent create(String workflowId) {
    StartWorkflowJobEvent jobEvent = new StartWorkflowJobEvent();
    jobEvent.setWorkflowId(workflowId);
    return jobEvent;
  }

  @Override
  public Type getType() {
    return Type.START_WORKFLOW_JOB_EVENT;
  }
}
