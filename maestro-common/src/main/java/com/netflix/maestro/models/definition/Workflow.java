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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.validations.MaestroIdConstraint;
import com.netflix.maestro.validations.SignalTriggerConstraint;
import com.netflix.maestro.validations.TagListConstraint;
import com.netflix.maestro.validations.TimeTriggerConstraint;
import com.netflix.maestro.validations.TimeoutConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * This is the workflow data model for a versioned workflow definition.
 *
 * <p>Scheduler users define their workflows, e.g. DAG, steps, using DSL and then send the data to
 * Maestro core following this data model.
 *
 * <p>Maestro also returns it when a workflow version is fetched by users over API.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "id",
      "name",
      "description",
      "tags",
      "timeout",
      "time_triggers",
      "signal_triggers",
      "criticality",
      "instance_step_concurrency",
      "params",
      "steps"
    },
    alphabetic = true)
@JsonDeserialize(builder = Workflow.WorkflowBuilder.class)
@Getter
@EqualsAndHashCode
public class Workflow {
  @MaestroIdConstraint private final String id;

  /**
   * Name of the workflow. Can be absent by user. Can be filled by workflow id if absent - use
   * helper method in WorkflowHelper.
   */
  @Size(max = Constants.NAME_LENGTH_LIMIT)
  private final String name;

  @Size(max = Constants.FIELD_SIZE_LIMIT)
  private final String description;

  @Valid @TagListConstraint private final TagList tags;

  /** Workflow timeout in seconds. */
  @TimeoutConstraint private final ParsableLong timeout;

  @Valid private final List<@TimeTriggerConstraint TimeTrigger> timeTriggers;
  @Valid private final List<@SignalTriggerConstraint SignalTrigger> signalTriggers;
  private final Criticality criticality;
  private final Long instanceStepConcurrency; // null means unset and disabled

  @Valid private final Map<String, ParamDefinition> params;

  @Valid private final List<Step> steps;

  @Builder(toBuilder = true)
  Workflow(
      String id,
      @Size(max = Constants.NAME_LENGTH_LIMIT) String name,
      @Size(max = Constants.FIELD_SIZE_LIMIT) String description,
      @Valid TagList tags,
      ParsableLong timeout,
      @Valid List<TimeTrigger> timeTriggers,
      @Valid List<SignalTrigger> signalTriggers,
      Criticality criticality,
      Long instanceStepConcurrency,
      @Valid Map<String, ParamDefinition> params,
      @Valid List<Step> steps) {
    this.id = id;
    this.name = name;
    this.description = description;
    if (tags != null) {
      this.tags = new TagList(null);
      this.tags.merge(tags.getTags()); // deep copy
    } else {
      this.tags = null;
    }
    this.timeout = timeout;
    this.timeTriggers = timeTriggers;
    this.signalTriggers = signalTriggers;
    this.criticality = criticality;
    this.instanceStepConcurrency = instanceStepConcurrency;
    this.params = ParamDefinition.preprocessDefinitionParams(params);
    this.steps = steps;
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class WorkflowBuilder {}

  @JsonIgnore
  public Map<String, StepTransition> getDag() {
    return steps.stream().collect(MapHelper.toListMap(Step::getId, Step::getTransition));
  }

  @JsonIgnore
  public List<String> getAllStepIds() {
    List<String> allStepIds = new ArrayList<>(steps.size());
    getAllStepIds(steps, allStepIds);
    return allStepIds;
  }

  private void getAllStepIds(List<Step> stepList, List<String> stepIds) {
    stepList.forEach(
        step -> {
          stepIds.add(step.getId());
          if (step.getType() == StepType.FOREACH) {
            getAllStepIds(((ForeachStep) step).getSteps(), stepIds);
          } else if (step.getType() == StepType.WHILE) {
            getAllStepIds(((WhileStep) step).getSteps(), stepIds);
          }
        });
  }

  /**
   * Returns workflow id if workflow name is missing and was not provided by the user.
   *
   * @return workflow name
   */
  @JsonIgnore
  public String getWorkflowNameOrDefault() {
    return getName() != null ? getName() : getId();
  }
}
