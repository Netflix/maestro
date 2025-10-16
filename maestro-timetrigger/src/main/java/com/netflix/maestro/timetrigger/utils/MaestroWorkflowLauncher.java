/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.timetrigger.utils;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.timetrigger.Constants;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import com.netflix.maestro.utils.IdHelper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

/** Utility for time trigger to launch maestro workflow instances. */
@AllArgsConstructor
public class MaestroWorkflowLauncher {
  private final WorkflowActionHandler actionHandler;

  /**
   * Starts the maestro workflow.
   *
   * @param workflowId the workflow id
   * @param version the workflow version
   * @param workflowTriggerUuid the workflow trigger uuid
   * @param executionBatch the list of planned time trigger executions
   * @return the list of run responses
   */
  public List<RunResponse> startWorkflowBatchRuns(
      String workflowId,
      String version,
      String workflowTriggerUuid,
      List<PlannedTimeTriggerExecution> executionBatch) {
    return actionHandler.startBatch(
        workflowId,
        version,
        createWorkflowRunRequests(workflowId, executionBatch, workflowTriggerUuid));
  }

  private List<RunRequest> createWorkflowRunRequests(
      String workflowId,
      List<PlannedTimeTriggerExecution> plannedExecutions,
      String workflowTriggerUuid) {
    return plannedExecutions.stream()
        .map(e -> createWorkflowRunRequest(workflowId, e, workflowTriggerUuid))
        .toList();
  }

  private RunRequest createWorkflowRunRequest(
      String workflowId, PlannedTimeTriggerExecution plannedExecution, String workflowTriggerUuid) {
    Instant executionInstance = plannedExecution.executionDate().toInstant();
    TimeTriggerWithWatermark timeTrigger = plannedExecution.timeTriggerWithWatermark();

    TimeInitiator timeInitiator = new TimeInitiator();
    timeInitiator.setTriggerUuid(workflowTriggerUuid);
    timeInitiator.setTriggerTime(executionInstance.toEpochMilli());
    timeInitiator.setTimezone(timeTrigger.getTimeTrigger().getTimezone());

    Map<String, ParamDefinition> runParams = new LinkedHashMap<>();
    runParams.put(
        Constants.MAESTRO_RUN_PARAM_NAME,
        LongParamDefinition.builder()
            .name(Constants.MAESTRO_RUN_PARAM_NAME)
            .value(executionInstance.toEpochMilli())
            .build());

    return RunRequest.builder()
        .initiator(timeInitiator)
        .requestTime(System.currentTimeMillis())
        .requestId(
            IdHelper.createUuid(
                String.format(
                    "%s:%s:%d",
                    workflowId,
                    timeTrigger.getTimeTrigger().toString(),
                    executionInstance.getEpochSecond())))
        .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
        .runParams(runParams)
        .persistFailedRun(true)
        .build();
  }
}
