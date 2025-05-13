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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.utils.Checks;
import jakarta.validation.constraints.NotNull;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Workflow run strategy, which applies to the whole workflow across all versions. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"rule", "workflow_concurrency"},
    alphabetic = true)
@Getter
@ToString
@EqualsAndHashCode
public class RunStrategy {

  @NotNull private final Rule rule;

  /** When it is 0, the workflow is paused. */
  private final long workflowConcurrency;

  /** Supported run strategy rules. */
  public enum Rule {
    /**
     * SEQUENTIAL: run workflows in the order they are created. One at a time - the earliest first.
     */
    SEQUENTIAL,
    /**
     * STRICT_SEQUENTIAL: run workflows if no error in the previous workflow run. Maestro runs
     * workflows in the sequential order as they are created. If the latest workflow run is failed,
     * the next workflow instances will be queued until the users manually restart the latest failed
     * run or unblock the workflow.
     */
    STRICT_SEQUENTIAL,
    /**
     * FIRST_ONLY: make sure the running workflow completes before queueing a new workflow instance.
     * Once a new workflow instance is created, if there is an existing workflow instance running,
     * the new one will stop itself.
     */
    FIRST_ONLY,
    /**
     * LAST_ONLY: make sure the running workflow is the latest created one. Keep only the last
     * instance running. Once a new workflow instance is created, if there is an existing workflow
     * instance running, the existing one will stop itself and the new one will run.
     */
    LAST_ONLY,
    /**
     * PARALLEL: run all triggered workflows in parallel constrained by the workflow concurrency
     * setting.
     */
    PARALLEL;

    /** Static creator. */
    @JsonCreator
    public static Rule create(String rule) {
      if (rule == null) {
        return null;
      }
      return Rule.valueOf(rule.toUpperCase(Locale.US));
    }
  }

  /** Run strategy constructor. */
  @JsonCreator
  public RunStrategy(
      @JsonProperty("rule") Rule rule,
      @JsonProperty("workflow_concurrency") Long workflowConcurrency) {
    Checks.notNull(rule, "rule in run strategy cannot be null");
    long concurrency;
    if (workflowConcurrency == null) {
      concurrency = (rule == Rule.PARALLEL ? Defaults.DEFAULT_PARALLELISM : 1L);
    } else {
      concurrency = workflowConcurrency;
    }
    Checks.checkTrue(
        concurrency <= 1 || rule == Rule.PARALLEL,
        "Only PARALLEL run strategy allows a workflow_concurrency greater than 1 ");
    this.rule = rule;
    this.workflowConcurrency = concurrency;
  }

  /**
   * Static method to create a RunStrategy by a rule.
   *
   * @param rule run strategy rule in a string
   * @return a RunStrategy object
   */
  @JsonCreator
  public static RunStrategy create(String rule) {
    Rule r = Rule.create(rule);
    return new RunStrategy(r, null);
  }

  /**
   * Static method to create a RunStrategy by a concurrency limit.
   *
   * @param concurrency workflow concurrency limit
   * @return a RunStrategy object
   */
  @JsonCreator
  public static RunStrategy create(long concurrency) {
    return new RunStrategy(Rule.PARALLEL, concurrency);
  }
}
