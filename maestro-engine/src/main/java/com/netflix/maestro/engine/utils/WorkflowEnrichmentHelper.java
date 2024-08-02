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

import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.definition.WorkflowDefinitionExtras;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.TriggerHelper;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/** Helper to enrich workflow definition. */
@AllArgsConstructor
public class WorkflowEnrichmentHelper {

  private final ParamsManager paramsManager;
  private final Map<StepType, StepRuntime> stepRuntimeMap;

  /**
   * Enrich workflow definition object with extras and add params, next run date and other enriched
   * fields.
   *
   * @param workflowDefinition workflow definition
   */
  public void enrichWorkflowDefinition(WorkflowDefinition workflowDefinition) {
    WorkflowDefinitionExtras enrichedWorkflowDefinition = new WorkflowDefinitionExtras();
    enrichParams(workflowDefinition, enrichedWorkflowDefinition);
    enrichNextRunDate(workflowDefinition, enrichedWorkflowDefinition);
    workflowDefinition.setEnrichedExtras(enrichedWorkflowDefinition);
  }

  private void enrichParams(
      WorkflowDefinition workflowDefinition, WorkflowDefinitionExtras enrichedWorkflowDefinition) {
    Map<String, ParamDefinition> staticParams =
        paramsManager.generatedStaticWorkflowParamDefs(workflowDefinition.getWorkflow());
    enrichedWorkflowDefinition.setWorkflowParams(staticParams);
    Map<String, Map<String, ParamDefinition>> stepParams = new LinkedHashMap<>();
    enrichedWorkflowDefinition.setStepParams(stepParams);
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(new LinkedHashMap<>());
    for (Step step : workflowDefinition.getWorkflow().getSteps()) {
      Map<String, ParamDefinition> params =
          paramsManager.generateStaticStepParamDefs(
              workflowSummary, step, stepRuntimeMap.get(step.getType()));
      stepParams.put(step.getId(), params);
    }
  }

  private void enrichNextRunDate(
      WorkflowDefinition workflowDefinition, WorkflowDefinitionExtras enrichedWorkflowExtras) {
    if (workflowDefinition.getWorkflow().getTimeTriggers() == null) {
      return;
    }
    Date now = new Date();
    Optional<ZonedDateTime> earliestDate =
        workflowDefinition.getWorkflow().getTimeTriggers().stream()
            .map(
                trigger ->
                    TriggerHelper.nextExecutionDate(
                        trigger, now, workflowDefinition.getWorkflow().getId()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .min(Comparator.naturalOrder());
    earliestDate.ifPresent(
        date -> enrichedWorkflowExtras.setNextExecutionTime(date.toEpochSecond()));

    enrichedWorkflowExtras.setNextExecutionTimes(
        workflowDefinition.getWorkflow().getTimeTriggers().stream()
            .map(
                trigger ->
                    TriggerHelper.nextExecutionDate(
                        trigger, now, workflowDefinition.getWorkflow().getId()))
            .map(date -> date.map(ZonedDateTime::toEpochSecond).orElse(null))
            .collect(Collectors.toList()));
  }
}
