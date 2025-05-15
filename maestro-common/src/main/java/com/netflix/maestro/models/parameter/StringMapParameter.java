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
import jakarta.validation.Valid;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * STRING_Map parameter instance to support Java Map<String, String> type param value. This
 * parameter type is a shortcut for users to quickly define a flat map structure.
 *
 * <p>SHOULD NOT mutate the evaluated string map data.
 */
@SuppressWarnings("unchecked")
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
@JsonDeserialize(builder = StringMapParameter.StringMapParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class StringMapParameter extends AbstractParameter {
  @Valid private final Map<String, String> value;
  private Map<String, String> evaluatedResult;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class StringMapParameterBuilderImpl
      extends StringMapParameterBuilder<StringMapParameter, StringMapParameterBuilderImpl> {
    @Override
    public StringMapParameter build() {
      StringMapParameter param = new StringMapParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    this.evaluatedResult = (Map<String, String>) result;
  }

  @Override
  public String getEvaluatedResultString() {
    return evaluatedResult.toString();
  }

  @Override
  public StringMapParameter asStringMapParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.STRING_MAP;
  }

  @Override
  public ParamDefinition toDefinition() {
    StringMapParamDefinition.StringMapParamDefinitionBuilder<?, ?> builder =
        StringMapParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(evaluatedResult).build();
  }
}
