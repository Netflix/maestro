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
package com.netflix.maestro.engine.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Step action DB object. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "action",
      "user",
      "reason",
      "workflow_action",
      "create_time",
      "run_params",
      "restart_config"
    },
    alphabetic = true)
@JsonDeserialize(builder = StepAction.StepActionBuilder.class)
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class StepAction {
  private final String workflowId;
  private final long workflowInstanceId;
  private final long workflowRunId;
  private final String stepId;
  private final Actions.StepInstanceAction action;
  private final User user;
  @Nullable private final String reason;
  // flag to indicate if this step action is a part of the workflow action.
  @Nullable private final boolean workflowAction;
  @Setter private Long createTime;

  // policy used to restart this step is always restart from specific step.
  @Nullable private final Map<String, ParamDefinition> runParams; // step run params
  @Nullable private final RestartConfig restartConfig;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class StepActionBuilder {
    /** build runParams. */
    public StepActionBuilder runParams(Map<String, ParamDefinition> input) {
      this.runParams = ParamDefinition.preprocessDefinitionParams(input);
      return this;
    }
  }

  /** static method to create a restart action. */
  public static StepAction createRestart(StepInstance stepInstance, RunRequest runRequest) {
    return create(
        Actions.StepInstanceAction.RESTART,
        stepInstance,
        runRequest.getRequester(),
        null,
        false,
        runRequest);
  }

  /** static method to create a termination action. */
  public static StepAction createTerminate(
      Actions.StepInstanceAction action,
      StepInstance stepInstance,
      User user,
      String reason,
      boolean workflowAction) {
    return create(action, stepInstance, user, reason, workflowAction, null);
  }

  /** static method to create a bypass step dependencies action. */
  public static StepAction createBypassStepDependencies(StepInstance stepInstance, User user) {
    return create(
        Actions.StepInstanceAction.BYPASS_STEP_DEPENDENCIES, stepInstance, user, null, false, null);
  }

  private static StepAction create(
      Actions.StepInstanceAction action,
      StepInstance stepInstance,
      User user,
      @Nullable String reason,
      @Nullable boolean workflowAction,
      @Nullable RunRequest runRequest) {
    StepActionBuilder builder =
        StepAction.builder()
            .workflowId(stepInstance.getWorkflowId())
            .workflowInstanceId(stepInstance.getWorkflowInstanceId())
            .workflowRunId(stepInstance.getWorkflowRunId())
            .stepId(stepInstance.getStepId())
            .action(action)
            .user(user)
            .reason(reason)
            .workflowAction(workflowAction);
    if (runRequest != null) {
      Checks.checkTrue(
          runRequest.getCurrentPolicy() == RunPolicy.RESTART_FROM_SPECIFIC,
          "Invalid step restart policy [%s] for step %s",
          runRequest.getCurrentPolicy(),
          stepInstance.getIdentity());
      Map<String, ParamDefinition> mergedRunParams =
          new LinkedHashMap<>(
              ObjectHelper.valueOrDefault(runRequest.getRunParams(), Collections.emptyMap()));
      mergedRunParams.putAll(
          MapHelper.getOrDefault(
              runRequest.getStepRunParams(), stepInstance.getStepId(), Collections.emptyMap()));
      builder.runParams(mergedRunParams);
      builder.restartConfig(runRequest.getRestartConfig());
    }
    return builder.build();
  }

  /** convert an action to timeline event. */
  public TimelineEvent toTimelineEvent() {
    if (reason == null) {
      return TimelineLogEvent.info(
          "User [%s] take action [%s] on the step", user.getName(), action.name());
    }
    return TimelineLogEvent.info(
        "User [%s] take action [%s] on the step due to reason: [%s]",
        user.getName(), action.name(), reason);
  }

  /** Get its unique info. */
  @JsonIgnore
  public UpstreamInitiator.Info toInfo() {
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId(workflowId);
    info.setInstanceId(workflowInstanceId);
    info.setRunId(workflowRunId);
    info.setStepId(stepId);
    return info;
  }

  /**
   * Get the terminal step status for a termination action (KILL, STOP, SKIP). Note that the
   * termination action can come from the upstream. If it is from the upstream, then the status is
   * STOPPED because the upstream action will stop the downstream running steps.
   */
  @JsonIgnore
  public StepInstance.Status getTerminalStatus(String wfId, long instanceId) {
    Checks.checkTrue(
        action.isUsingUpstream(),
        "[%s][%s] cannot getTerminalStatus for action [%s], which does not support upstream mode",
        wfId,
        instanceId,
        this);
    if (Objects.equals(workflowId, wfId) && workflowInstanceId == instanceId) {
      return action.getStatus();
    }
    return StepInstance.Status.STOPPED;
  }
}
