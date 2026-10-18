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
package com.netflix.maestro.engine.templates;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the step list of a template execution instance from the prepared steps: adds the steps the
 * workflow supplied where their own transitions place them and appends the collector step that
 * evaluates the registered output params.
 *
 * <p>A supplied step keeps its successors as written. Each of its predecessors gains an
 * unconditional edge to it, so the final DAG is the prepared DAG plus the supplied steps and their
 * edges; no prepared edge is removed. Predecessor lists are cleared afterwards and derived again
 * when the instance is created. The steps passed in are mutated, so callers pass copies.
 */
public final class TemplateStepSplicer {
  private static final String ALWAYS = "true";
  private static final String UNKNOWN_STEP =
      "Template step input [%s] names %s [%s], which is neither a prepared step nor a step input";

  private TemplateStepSplicer() {}

  /**
   * Add every supplied step to the prepared steps. Every predecessor and successor a supplied step
   * names must be a prepared step or another supplied step.
   *
   * @param prepared the prepared steps, mutated in place
   * @param stepInputs the supplied steps, possibly null, mutated in place
   * @return the step list of the execution instance without the collector
   */
  public static List<Step> splice(List<Step> prepared, List<Step> stepInputs) {
    List<Step> steps = new ArrayList<>(prepared);
    if (stepInputs != null && !stepInputs.isEmpty()) {
      Map<String, Step> byId = new LinkedHashMap<>();
      for (Step step : steps) {
        byId.put(step.getId(), step);
      }
      for (Step supplied : stepInputs) {
        Checks.checkTrue(
            !byId.containsKey(supplied.getId()),
            "Template step input [%s] collides with a prepared step id or another step input",
            supplied.getId());
        byId.put(supplied.getId(), supplied);
      }
      for (Step supplied : stepInputs) {
        Checks.checkTrue(
            !supplied.getTransition().getPredecessors().isEmpty()
                || !supplied.getTransition().getSuccessors().isEmpty(),
            "Template step input [%s] names no predecessor or successor",
            supplied.getId());
        for (String successor : supplied.getTransition().getSuccessors().keySet()) {
          Checks.checkTrue(
              byId.containsKey(successor), UNKNOWN_STEP, supplied.getId(), "successor", successor);
        }
        for (String predecessor : supplied.getTransition().getPredecessors()) {
          Step from =
              Checks.notNull(
                  byId.get(predecessor),
                  UNKNOWN_STEP,
                  supplied.getId(),
                  "predecessor",
                  predecessor);
          from.getTransition().getSuccessors().putIfAbsent(supplied.getId(), ALWAYS);
        }
      }
      steps.addAll(stepInputs);
    }
    steps.forEach(step -> step.getTransition().setPredecessors(new ArrayList<>()));
    return steps;
  }

  /**
   * Append the collector step carrying the output params and point every step without successors to
   * it.
   *
   * @param steps the spliced steps, mutated in place
   * @param outputParams the registered output params, possibly null
   * @return the final step list of the execution instance
   */
  public static List<Step> appendCollector(
      List<Step> steps, Map<String, ParamDefinition> outputParams) {
    Set<String> ids = new HashSet<>();
    steps.forEach(step -> ids.add(step.getId()));
    Checks.checkTrue(
        !ids.contains(Constants.TEMPLATE_OUTPUTS_STEP_ID),
        "Template step id [%s] is reserved for the output collector",
        Constants.TEMPLATE_OUTPUTS_STEP_ID);
    TypedStep collector = new TypedStep();
    collector.setId(Constants.TEMPLATE_OUTPUTS_STEP_ID);
    collector.setType(StepType.NOOP);
    collector.setParams(outputParams == null ? new LinkedHashMap<>() : outputParams);
    for (Step step : steps) {
      if (step.getTransition().getSuccessors().isEmpty()) {
        step.getTransition().getSuccessors().put(Constants.TEMPLATE_OUTPUTS_STEP_ID, ALWAYS);
      }
    }
    steps.add(collector);
    return steps;
  }
}
