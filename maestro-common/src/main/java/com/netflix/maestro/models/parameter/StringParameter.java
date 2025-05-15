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
package com.netflix.maestro.models.parameter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.utils.Checks;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/** STRING Parameter instance. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "name",
      "value",
      "expression",
      "type",
      "validator",
      "tags",
      "mode",
      "evaluated_result",
      "evaluated_time"
    },
    alphabetic = true)
@JsonDeserialize(builder = StringParameter.StringParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class StringParameter extends AbstractParameter {
  private final String value;
  private String evaluatedResult;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class StringParameterBuilderImpl
      extends StringParameterBuilder<StringParameter, StringParameterBuilderImpl> {
    @Override
    public StringParameter build() {
      StringParameter param = new StringParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    Checks.checkTrue(
        result instanceof String
            || result instanceof Long
            || result instanceof Double
            || result instanceof Boolean,
        "Param [%s] is expected to be a String compatible type but is [%s]",
        getName(),
        result.getClass());
    if (!(result instanceof String)) {
      this.getMeta()
          .put(
              "info",
              "Implicitly converted the evaluated result to a string for type "
                  + result.getClass());
    }
    this.evaluatedResult = result.toString();
  }

  @Override
  public String getEvaluatedResultString() {
    return evaluatedResult;
  }

  @Override
  public StringParameter asStringParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.STRING;
  }

  @Override
  public ParamDefinition toDefinition() {
    StringParamDefinition.StringParamDefinitionBuilder<?, ?> builder =
        StringParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(evaluatedResult).build();
  }
}
