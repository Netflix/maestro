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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Workflow rollup leaf step instances' overview at runtime. This holds aggregated rollup - meaning
 * it is a rollup information that's aggregated across runs from run1 to the current run. For
 * example, for an instance with 7 steps in total, if run1 had 3 steps succeeded and 1 failed, and
 * run2 had 4 steps succeeded after restarting instance from incomplete, aggregated rollup for run2
 * will contain 7 succeeded steps.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"total_leaf_count", "overview"},
    alphabetic = true)
@Data
public class WorkflowRollupOverview {
  private long totalLeafCount;

  private EnumMap<StepInstance.Status, CountReference> overview;

  /** class to include a count and the references. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {"cnt", "ref"},
      alphabetic = true)
  @Data
  public static class CountReference {
    private int cnt;
    private Map<String, List<String>> ref;

    @Override
    public int hashCode() {
      return cnt;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof CountReference)) {
        return false;
      }
      final CountReference other = (CountReference) obj;
      return this.cnt == other.cnt;
    }

    @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
    private void aggregate(CountReference other) {
      this.cnt += other.cnt;
      if (other.ref != null) {
        if (this.ref == null) {
          this.ref = new LinkedHashMap<>();
        }
        for (Map.Entry<String, List<String>> entry : other.ref.entrySet()) {
          if (!this.ref.containsKey(entry.getKey())) {
            this.ref.put(entry.getKey(), new ArrayList<>());
          }
          this.ref.get(entry.getKey()).addAll(entry.getValue());
        }
      }
    }

    private void segregate(CountReference other) {
      this.cnt -= other.cnt;
      if (other.ref != null) {
        Checks.notNull(this.ref, "Invalid: full refs must include the segregation refs");
        other.ref.forEach(
            (k, v) -> {
              Checks.notNull(
                      this.ref.get(k), "Invalid: a total ref must include the segregation ref")
                  .removeAll(v);
              if (this.ref.get(k).isEmpty()) {
                this.ref.remove(k);
              }
            });
      }
    }
  }

  /** An entity class to represent a decoded reference. */
  @Getter
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class ReferenceEntity {
    private final String workflowId;
    private final long instanceId;
    private final long runId;
    private final String stepId;
    private final long attemptId;

    /** decode a reference key and reference value. */
    public static ReferenceEntity decode(String refKey, String refVal) {
      String[] part1 = refKey.split(Constants.REFERENCE_DELIMITER);
      String[] part2 = refVal.split(Constants.REFERENCE_DELIMITER);
      Checks.checkTrue(
          part1.length == 2 && part2.length == 1 + part1.length,
          "Invalid reference <key, value>: [%s][%s]",
          refKey,
          refVal);
      return new ReferenceEntity(
          part1[0],
          Long.parseLong(part2[0]),
          Long.parseLong(part1[1]),
          part2[1],
          Long.parseLong(part2[2]));
    }
  }

  /** Static method to create a RuntimeOverview. */
  public static WorkflowRollupOverview of(
      long totalLeafStepCount, Map<StepInstance.Status, CountReference> overview) {
    WorkflowRollupOverview rollupOverview = new WorkflowRollupOverview();
    rollupOverview.totalLeafCount = totalLeafStepCount;
    rollupOverview.overview = new EnumMap<>(StepInstance.Status.class);
    rollupOverview.overview.putAll(overview);
    return rollupOverview;
  }

  /** check if runtime overview is the same. */
  public static boolean isSame(WorkflowRollupOverview one, WorkflowRollupOverview another) {
    if (Objects.equals(one, another)) {
      return true;
    }
    if (one != null && another != null) {
      return one.totalLeafCount == another.totalLeafCount
          && Objects.equals(one.overview, another.overview);
    }
    return false;
  }

  /** aggregate rollup overview into this and return itself. */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  @JsonIgnore
  public WorkflowRollupOverview aggregate(WorkflowRollupOverview rollup) {
    if (rollup == null) {
      return this;
    }
    this.totalLeafCount += rollup.totalLeafCount;
    if (rollup.overview == null) {
      return this;
    }

    if (this.overview == null) {
      this.overview = new EnumMap<>(StepInstance.Status.class);
    }
    for (Map.Entry<StepInstance.Status, CountReference> entry : rollup.overview.entrySet()) {
      if (!this.overview.containsKey(entry.getKey())) {
        this.overview.put(entry.getKey(), new CountReference());
      }
      this.overview.get(entry.getKey()).aggregate(entry.getValue());
    }
    return this;
  }

  /** segregate/subtract the input rollup overview from itself. */
  @JsonIgnore
  void segregate(WorkflowRollupOverview rollup) {
    if (rollup == null) {
      return;
    }
    this.totalLeafCount -= rollup.totalLeafCount;
    if (rollup.overview == null) {
      return;
    }

    rollup.overview.forEach(
        (k, v) -> {
          Checks.notNull(
                  this.overview.get(k),
                  "Invalid: full overview should include the segregation rollup")
              .segregate(v);
          if (this.overview.get(k).cnt == 0) {
            this.overview.remove(k);
          }
        });
  }
}
