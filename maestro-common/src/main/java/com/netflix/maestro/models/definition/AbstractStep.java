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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.validations.MaestroReferenceIdConstraint;
import com.netflix.maestro.validations.StepDependenciesDefinitionConstraint;
import com.netflix.maestro.validations.TagListConstraint;
import com.netflix.maestro.validations.TimeoutConstraint;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.Getter;

/** Abstract base step class to include shared fields for all kinds of steps. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public abstract class AbstractStep implements Step {
  @Getter(onMethod = @__({@Override}))
  @MaestroReferenceIdConstraint
  private String id;

  @Getter(onMethod = @__({@Override}))
  @Size(max = Constants.NAME_LENGTH_LIMIT)
  private String name;

  @Getter(onMethod = @__({@Override}))
  @Size(max = Constants.FIELD_SIZE_LIMIT)
  private String description;

  @Getter(onMethod = @__({@Override}))
  @Valid
  private StepTransition transition = new StepTransition();

  @Getter(onMethod = @__({@Override}))
  private FailureMode failureMode; // null means unset

  @Getter(onMethod = @__({@Override}))
  @Valid
  @TagListConstraint
  private TagList tags;

  @Getter(onMethod = @__({@Override}))
  @TimeoutConstraint
  private ParsableLong timeout;

  @Valid
  private Map<StepDependencyType, @StepDependenciesDefinitionConstraint StepDependenciesDefinition>
      dependencies;

  @Valid private Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> outputs;

  @Getter(onMethod = @__({@Override}))
  @Valid
  private Map<String, ParamDefinition> params;

  /** set params. */
  public void setParams(Map<String, ParamDefinition> input) {
    this.params = ParamDefinition.preprocessDefinitionParams(input);
  }

  @JsonIgnore
  @Override
  public RetryPolicy getRetryPolicy() {
    return Defaults.DEFAULT_RETRY_POLICY;
  }
}
