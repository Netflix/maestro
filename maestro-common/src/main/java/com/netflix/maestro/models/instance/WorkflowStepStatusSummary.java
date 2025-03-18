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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Class to hold step overview, including start time and end time, for all step instances in a given
 * status. The steps in the list must be sorted by the ordinal id (the first element in the list).
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"cnt", "steps"},
    alphabetic = true)
@Data
public class WorkflowStepStatusSummary {
  private long cnt; // number of step instances in a given status.

  // each step includes step ordinal id and start time and end time of the latest step attempt
  @Nullable private List<List<Long>> steps;

  /** Static creator for JSON. */
  @JsonCreator
  public static WorkflowStepStatusSummary of(long count) {
    WorkflowStepStatusSummary overview = new WorkflowStepStatusSummary();
    overview.setCnt(count);
    return overview;
  }

  /** Add a step into the summary. */
  @JsonIgnore
  public WorkflowStepStatusSummary addStep(@NotNull List<Long> stepInfo) {
    if (steps == null) {
      steps = new ArrayList<>();
    }
    steps.add(stepInfo);
    cnt = steps.size();
    return this;
  }

  /** Make sure the steps are sorted by their ordinal ids. */
  @JsonIgnore
  public void sortSteps() {
    if (steps != null) {
      steps.sort(Comparator.comparingLong(List::getFirst));
    }
  }
}
