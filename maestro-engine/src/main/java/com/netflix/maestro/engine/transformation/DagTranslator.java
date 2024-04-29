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
package com.netflix.maestro.engine.transformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/** Workflow DAG translator to transform a workflow instance to a DAG. */
@AllArgsConstructor
public class DagTranslator implements Translator<WorkflowInstance, Map<String, StepTransition>> {
  private final ObjectMapper objectMapper;

  /**
   * Translate a workflow instance to a DAG with step id and step transitions.
   *
   * @param workflowInstance instance will be cloned and then translated.
   * @return the dag
   */
  @Override
  public Map<String, StepTransition> translate(WorkflowInstance workflowInstance) {
    WorkflowInstance instance = objectMapper.convertValue(workflowInstance, WorkflowInstance.class);

    if (instance.getRunConfig() != null) {
      if (instance.getRunConfig().getPolicy() == RunPolicy.RESTART_FROM_INCOMPLETE
          || instance.getRunConfig().getPolicy() == RunPolicy.RESTART_FROM_SPECIFIC) {
        Map<String, StepInstance.Status> statusMap =
            instance.getAggregatedInfo().getStepAggregatedViews().entrySet().stream()
                .collect(
                    Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getStatus()));
        if (!statusMap.isEmpty()) {
          instance
              .getRunConfig()
              .setStartStepIds(
                  statusMap.entrySet().stream()
                      .filter(
                          entry ->
                              !entry.getValue().isComplete()
                                  && (entry.getValue().isTerminal()
                                      || entry.getValue() == StepInstance.Status.NOT_CREATED))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toList()));
        }
        // handle the special case of restarting from a completed step
        if (instance.getRunConfig().getPolicy() == RunPolicy.RESTART_FROM_SPECIFIC) {
          String restartStepId =
              RunRequest.getCurrentNode(instance.getRunConfig().getRestartConfig()).getStepId();
          if (!instance.getRunConfig().getStartStepIds().contains(restartStepId)) {
            instance.getRunConfig().getStartStepIds().add(restartStepId);
          }
        }
      } else {
        if (workflowInstance.getRunConfig().getStartStepIds() != null) {
          instance
              .getRunConfig()
              .setStartStepIds(new ArrayList<>(workflowInstance.getRunConfig().getStartStepIds()));
        }
        if (workflowInstance.getRunConfig().getEndStepIds() != null) {
          instance
              .getRunConfig()
              .setEndStepIds(new ArrayList<>(workflowInstance.getRunConfig().getEndStepIds()));
        }
      }
    }

    List<String> startStepIds =
        instance.getRunConfig() != null && instance.getRunConfig().getStartStepIds() != null
            ? instance.getRunConfig().getStartStepIds()
            : null;
    List<String> endStepIds =
        instance.getRunConfig() != null && instance.getRunConfig().getEndStepIds() != null
            ? instance.getRunConfig().getEndStepIds()
            : null;

    return WorkflowGraph.computeDag(instance.getRuntimeWorkflow(), startStepIds, endStepIds);
  }
}
