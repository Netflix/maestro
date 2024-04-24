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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.utils.Checks;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/** BOOLEAN Parameter instance. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
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
@JsonDeserialize(builder = BooleanParameter.BooleanParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class BooleanParameter extends AbstractParameter {
  private final Boolean value;
  private Boolean evaluatedResult;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class BooleanParameterBuilderImpl
      extends BooleanParameterBuilder<BooleanParameter, BooleanParameterBuilderImpl> {
    @Override
    public BooleanParameter build() {
      BooleanParameter param = new BooleanParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    Checks.checkTrue(
        result instanceof String || result instanceof Boolean,
        "Param [%s] is expected to be a Boolean compatible type but is [%s]",
        getName(),
        result.getClass());
    if (result instanceof String) {
      this.getMeta()
          .put("warn", "Implicitly converted the evaluated result to a boolean for type String");
      if ("true".equalsIgnoreCase((String) result)) {
        this.evaluatedResult = Boolean.TRUE;
      } else if ("false".equalsIgnoreCase((String) result)) {
        this.evaluatedResult = Boolean.FALSE;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Param [%s] is expected to have a Boolean compatible result but is [%s]",
                getName(), result));
      }
    } else {
      this.evaluatedResult = (Boolean) result;
    }
  }

  @Override
  public String getEvaluatedResultString() {
    return evaluatedResult.toString();
  }

  @Override
  public BooleanParameter asBooleanParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.BOOLEAN;
  }

  @Override
  public ParamDefinition toDefinition() {
    BooleanParamDefinition.BooleanParamDefinitionBuilder<?, ?> builder =
        BooleanParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(evaluatedResult).build();
  }
}
