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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.utils.Checks;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;

/** Workflow step instances' overview at runtime. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"total_step_count", "step_overview", "rollup_overview", "run_status"},
    alphabetic = true)
@Data
@SuppressWarnings("PMD.LooseCoupling")
public class WorkflowRuntimeOverview {
  private long totalStepCount;
  private EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stepOverview =
      new EnumMap<>(StepInstance.Status.class);

  /**
   * This holds aggregated info - meaning it is a rollup information that's aggregated across runs
   * from current run to run1.
   */
  private WorkflowRollupOverview rollupOverview;

  /**
   * keep the current workflow instance run status if different from instance status. Currently, the
   * only case is when runStatus succeeds but aggregated workflow instance status is not succeeded.
   */
  @Nullable private WorkflowInstance.Status runStatus;

  /** Static method to create a RuntimeOverview. */
  public static WorkflowRuntimeOverview of(
      long totalStepCount,
      EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stepOverview,
      WorkflowRollupOverview rollupOverview) {
    WorkflowRuntimeOverview overview = new WorkflowRuntimeOverview();
    overview.totalStepCount = totalStepCount;
    overview.stepOverview = stepOverview;
    overview.rollupOverview = rollupOverview;
    return overview;
  }

  /** check if runtime overview is the same. */
  public static boolean isSame(WorkflowRuntimeOverview one, WorkflowRuntimeOverview another) {
    if (Objects.equals(one, another)) {
      return true;
    }
    if (one != null && another != null) {
      return one.totalStepCount == another.totalStepCount
          && Objects.equals(one.stepOverview, another.stepOverview)
          && WorkflowRollupOverview.isSame(one.rollupOverview, another.rollupOverview);
    }
    return false;
  }

  /** check if there is any step in NOT_CREATED status. */
  public boolean existsNotCreatedStep() {
    return totalStepCount > getCreatedStepCount();
  }

  /** check if there is any step in NOT_CREATED status. */
  public boolean existsCreatedStep() {
    return 0 != getCreatedStepCount();
  }

  private long getCreatedStepCount() {
    return stepOverview.entrySet().stream()
        .filter(
            entry -> entry.getKey() != StepInstance.Status.NOT_CREATED && entry.getValue() != null)
        .map(entry -> entry.getValue().getCnt())
        .reduce(0L, Long::sum);
  }

  /**
   * Translate compressed stepOverview into human-readable format, key is step id and value is step
   * runtime state. Only takes into account steps from step overview and the runtime dag, does not
   * contain steps that were not created yet.
   *
   * @param runtimeDag workflow instance runtime dag, expected to be the one used for encoding.
   * @return a map of step id to its runtime state.
   */
  @JsonIgnore
  public Map<String, StepRuntimeState> decodeStepOverview(Map<String, StepTransition> runtimeDag) {
    AtomicLong ordinal = new AtomicLong(0);
    Map<Long, String> ordinalStepMap =
        runtimeDag.keySet().stream()
            .collect(Collectors.toMap(s -> ordinal.incrementAndGet(), Function.identity()));
    Map<String, StepRuntimeState> states = new LinkedHashMap<>();
    stepOverview.forEach(
        (status, summary) -> {
          if (summary.getSteps() != null) {
            summary
                .getSteps()
                .forEach(
                    stepInfo -> {
                      String stepId =
                          Checks.notNull(
                              ordinalStepMap.remove(stepInfo.getFirst()),
                              "cannot find step id for stepInfo [%s]",
                              stepInfo);
                      StepRuntimeState state = new StepRuntimeState();
                      state.setStatus(status);
                      state.setStartTime(stepInfo.get(1));
                      state.setEndTime(stepInfo.get(2));
                      states.put(stepId, state);
                    });
          }
        });
    return states;
  }
}
