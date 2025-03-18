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
package com.netflix.maestro.engine.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstanceTransition;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.signal.SignalDependencies;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.IdHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Utility class for step. */
@SuppressWarnings("unchecked")
public final class StepHelper {
  private static final String RUNTIME_STATE_FIELD = "runtime_state";
  private static final String STATUS_FIELD = "status";
  private static final String TRANSITION_FIELD = "transition";
  private static final String CONVERT_FIELD_ERROR = "Cannot find field [%s] in the data";

  private StepHelper() {}

  /**
   * Return step id in case step name is absent.
   *
   * @param step step definition
   * @return step name
   */
  public static String getStepNameOrDefault(Step step) {
    if (step.getName() != null) {
      return step.getName();
    } else {
      return step.getId();
    }
  }

  /**
   * Utility method to retrieve workflow summary from the key-value data.
   *
   * @param data all data in a map
   * @return a workflow summary object
   */
  public static WorkflowSummary retrieveWorkflowSummary(
      ObjectMapper objectMapper, Map<String, Object> data) {
    return convertField(
        objectMapper, data, Constants.WORKFLOW_SUMMARY_FIELD, WorkflowSummary.class);
  }

  /**
   * Utility method to retrieve workflow runtime summary from the key-value data.
   *
   * @param data all data in a map
   * @return a workflow summary object
   */
  public static WorkflowRuntimeSummary retrieveWorkflowRuntimeSummary(
      ObjectMapper objectMapper, Map<String, Object> data) {
    if (data.containsKey(Constants.WORKFLOW_RUNTIME_SUMMARY_FIELD)) {
      return convertField(
          objectMapper,
          data,
          Constants.WORKFLOW_RUNTIME_SUMMARY_FIELD,
          WorkflowRuntimeSummary.class);
    }
    return new WorkflowRuntimeSummary();
  }

  /**
   * Utility method to retrieve step runtime summary from the key-value data.
   *
   * @param data all data in a map
   * @return a step summary object
   */
  public static StepRuntimeSummary retrieveRuntimeSummary(
      ObjectMapper objectMapper, Map<String, Object> data) {
    return convertField(
        objectMapper, data, Constants.STEP_RUNTIME_SUMMARY_FIELD, StepRuntimeSummary.class);
  }

  /** utility to get the step dependencies summaries of a stepId. */
  public static SignalDependencies getSignalDependencies(Flow flow, String stepId) {
    if (flow.getPrepareTask().getOutputData().containsKey(Constants.ALL_STEP_DEPENDENCIES_FIELD)) {
      var allStepDependencies =
          (Map<String, SignalDependencies>)
              flow.getPrepareTask().getOutputData().get(Constants.ALL_STEP_DEPENDENCIES_FIELD);
      return allStepDependencies.get(stepId);
    }
    return null;
  }

  /**
   * Retrieve the step status from a map.
   *
   * @param data data contains step runtime summary, which has a status field.
   * @return status of the step instance.
   */
  public static StepInstance.Status retrieveStepStatus(Map<String, Object> data) {
    Object runtimeSummary =
        data.getOrDefault(Constants.STEP_RUNTIME_SUMMARY_FIELD, Collections.emptyMap());
    if (runtimeSummary instanceof StepRuntimeSummary) {
      return ((StepRuntimeSummary) runtimeSummary).getRuntimeState().getStatus();
    }
    return StepInstance.Status.valueOf(
        (String)
            ((Map<String, Object>)
                    ((Map<String, Object>) runtimeSummary)
                        .getOrDefault(RUNTIME_STATE_FIELD, Collections.emptyMap()))
                .getOrDefault(STATUS_FIELD, StepInstance.Status.CREATED.name()));
  }

  /**
   * Retrieve the step runtime state from a map.
   *
   * @param data data contains step runtime summary, which has a status field.
   * @return status of the step instance.
   */
  public static StepRuntimeState retrieveStepRuntimeState(
      Map<String, Object> data, ObjectMapper objectMapper) {
    Object runtimeSummary =
        data.getOrDefault(Constants.STEP_RUNTIME_SUMMARY_FIELD, Collections.emptyMap());
    if (runtimeSummary instanceof StepRuntimeSummary) {
      return ((StepRuntimeSummary) runtimeSummary).getRuntimeState();
    }
    Object state = ((Map<String, Object>) runtimeSummary).getOrDefault(RUNTIME_STATE_FIELD, null);
    if (state != null) {
      return objectMapper.convertValue(state, StepRuntimeState.class);
    }
    return new StepRuntimeState();
  }

  /**
   * Retrieve the step instance transition from a map.
   *
   * @param data data contains step runtime summary, which has a transition field
   * @return step instance transition object
   */
  public static StepInstanceTransition retrieveStepTransition(
      ObjectMapper objectMapper, Map<String, Object> data) {
    Object value = data.getOrDefault(Constants.STEP_RUNTIME_SUMMARY_FIELD, Collections.emptyMap());
    if (value instanceof StepRuntimeSummary) {
      return ((StepRuntimeSummary) value).getTransition();
    }
    return convertField(
        objectMapper, (Map<String, Object>) value, TRANSITION_FIELD, StepInstanceTransition.class);
  }

  /** Create workflow run requests within subworkflow and foreach steps. */
  public static RunRequest createInternalWorkflowRunRequest(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      List<Tag> tags,
      Map<String, ParamDefinition> runParams,
      String dedupKey) {

    UpstreamInitiator initiator =
        UpstreamInitiator.withType(Initiator.Type.valueOf(runtimeSummary.getType().name()));
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId(workflowSummary.getWorkflowId());
    parent.setInstanceId(workflowSummary.getWorkflowInstanceId());
    parent.setRunId(workflowSummary.getWorkflowRunId());
    parent.setStepId(runtimeSummary.getStepId());
    parent.setStepAttemptId(runtimeSummary.getStepAttemptId());

    List<UpstreamInitiator.Info> ancestors = new ArrayList<>();
    if (workflowSummary.getInitiator() instanceof UpstreamInitiator) {
      ancestors.addAll(((UpstreamInitiator) workflowSummary.getInitiator()).getAncestors());
    }
    ancestors.add(parent);
    initiator.setAncestors(ancestors);
    // enforce depth limit, e.g. 10, it can also prevent workflow-subworkflow cycle
    Checks.checkTrue(
        initiator.getDepth() < Constants.WORKFLOW_DEPTH_LIMIT,
        "Workflow initiator [%s] is not less than the depth limit: %s",
        initiator,
        Constants.WORKFLOW_DEPTH_LIMIT);

    return RunRequest.builder()
        .initiator(initiator)
        .requestId(IdHelper.createUuid(dedupKey))
        .requestTime(System.currentTimeMillis())
        .currentPolicy(workflowSummary.getRunPolicy()) // default and might be updated
        .runtimeTags(tags)
        .correlationId(workflowSummary.getCorrelationId())
        .groupInfo(workflowSummary.getGroupInfo())
        .instanceStepConcurrency(workflowSummary.getInstanceStepConcurrency()) // pass it down
        .runParams(runParams)
        .restartConfig(
            copyRestartConfigWithClonedPath(
                ObjectHelper.valueOrDefault(
                    runtimeSummary.getRestartConfig(),
                    workflowSummary.getRestartConfig()))) // to be updated
        .build();
  }

  private static RestartConfig copyRestartConfigWithClonedPath(RestartConfig toCopy) {
    if (toCopy != null) {
      return toCopy.toBuilder().restartPath(new ArrayList<>(toCopy.getRestartPath())).build();
    }
    return null;
  }

  /**
   * Utility method to convert an object in the field of the data to a given class type.
   *
   * @param data map of all data
   * @param fieldName map key to get the specific info
   * @param clazz class type to convert
   * @param <T> Type of converted object
   * @return a converted object
   */
  private static <T> T convertField(
      ObjectMapper objectMapper, Map<String, Object> data, String fieldName, Class<T> clazz) {
    Object value = Checks.notNull(data.get(fieldName), CONVERT_FIELD_ERROR, fieldName);
    if (clazz.isInstance(value)) {
      return (T) value;
    }
    return objectMapper.convertValue(value, clazz);
  }

  /** Returns the step type info - if subType is available, return it, else the step type. */
  public static String getStepTypeInfo(StepType stepType, String subType) {
    return Checks.isNullOrEmpty(subType) ? stepType.name() : subType;
  }
}
