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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Map;

/** Step interface. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "step", value = TypedStep.class),
  @JsonSubTypes.Type(name = "subworkflow", value = SubworkflowStep.class),
  @JsonSubTypes.Type(name = "foreach", value = ForeachStep.class),
  @JsonSubTypes.Type(name = "template", value = TemplateStep.class)
})
public interface Step {
  /** Get step id. */
  String getId();

  /**
   * Get step name. Could be absent by the user. Step id can be used when name is not provided - use
   * StepHelper for that.
   */
  String getName();

  /** Get step description. */
  String getDescription();

  /** Get step parameter definition. */
  Map<String, ParamDefinition> getParams();

  /** Get step tags. */
  TagList getTags();

  /** Get step timeout in seconds. */
  Duration getTimeout();

  /** Get step type. */
  StepType getType();

  /** Get step subtype. */
  default String getSubType() {
    return null;
  }

  /** Get step failure mode. */
  FailureMode getFailureMode();

  /** Get step transition info. */
  StepTransition getTransition();

  /** Get retry policy. */
  RetryPolicy getRetryPolicy();

  /** Get dependencies definition for different types like signals. */
  Map<StepDependencyType, StepDependenciesDefinition> getDependencies();

  /** Get step outputs, e.g signal outputs. */
  Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> getOutputs();
}
