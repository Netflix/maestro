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
package com.netflix.maestro.engine.validations;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroDryRunException;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.StepInstanceTransition;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.ExceptionHelper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 * Validator class to perform param dry run validation. Goes through merge / schema validation
 * process.
 */
@AllArgsConstructor
public class DryRunValidator {
  private static final int MAX_STACKTRACE_LINES = 20;
  private final Map<StepType, StepRuntime> stepRuntimeMap;
  private final DefaultParamManager defaultParamManager;
  private final ParamsManager paramsManager;
  private final WorkflowHelper workflowHelper;

  /** Special initiator to indicate calling from dry run validator. */
  public static class ValidationInitiator extends ManualInitiator {}

  /**
   * Run dry run validation for workflow and step parameters.
   *
   * @param workflow workflow version definition
   * @param caller the requester
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  public void validate(Workflow workflow, User caller) {
    try {
      RunProperties runProperties = new RunProperties();
      runProperties.setOwner(caller);
      Map<String, ParamDefinition> workflowParams = workflow.getParams();
      Map<String, ParamDefinition> defaultDryRunParams =
          defaultParamManager.getDefaultDryRunParams();
      // add run params to override params with known invalid defaults
      Map<String, ParamDefinition> filteredParams =
          defaultDryRunParams.entrySet().stream()
              .filter(
                  entry ->
                      workflowParams != null
                          && workflowParams.containsKey(entry.getKey())
                          && workflowParams.get(entry.getKey()).getType()
                              == entry.getValue().getType())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      Initiator initiator = new ValidationInitiator();
      initiator.setCaller(caller);
      RunRequest runRequest =
          RunRequest.builder()
              .initiator(initiator)
              .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
              .runParams(filteredParams)
              .build();

      WorkflowInstance workflowInstance =
          workflowHelper.createWorkflowInstance(workflow, 1L, 1L, runProperties, runRequest);
      WorkflowSummary workflowSummary =
          workflowHelper.createWorkflowSummaryFromInstance(workflowInstance);

      // todo: improve to traverse in DAG order to validate steps and their params
      for (Step step : workflow.getSteps()) {
        StepRuntimeSummary runtimeSummary =
            StepRuntimeSummary.builder()
                .stepId(step.getId())
                .stepAttemptId(1L)
                .stepInstanceId(1L)
                .stepInstanceUuid(UUID.randomUUID().toString())
                .stepName(StepHelper.getStepNameOrDefault(step))
                .tags(step.getTags())
                .type(step.getType())
                .subType(step.getSubType())
                .params(new LinkedHashMap<>())
                .transition(StepInstanceTransition.from(step))
                .synced(true)
                .build();
        paramsManager.generateMergedStepParams(
            workflowSummary, step, stepRuntimeMap.get(step.getType()), runtimeSummary);
      }
    } catch (Exception e) {
      throw new MaestroDryRunException(
          e,
          "Exception during dry run validation for workflow %s Error=[%s] Type=[%s] StackTrace=[%s]",
          workflow.getId(),
          e.getMessage(),
          e.getClass(),
          ExceptionHelper.getStackTrace(e, MAX_STACKTRACE_LINES));
    }
  }
}
