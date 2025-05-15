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
import java.math.BigDecimal;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/** DOUBLE Parameter instance. */
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
@JsonDeserialize(builder = DoubleParameter.DoubleParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class DoubleParameter extends AbstractParameter {
  private final BigDecimal value;
  private Double evaluatedResult;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class DoubleParameterBuilderImpl
      extends DoubleParameterBuilder<DoubleParameter, DoubleParameterBuilderImpl> {
    @Override
    public DoubleParameter build() {
      DoubleParameter param = new DoubleParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    if (result instanceof BigDecimal) {
      this.evaluatedResult = ((BigDecimal) result).doubleValue();
    } else {
      this.evaluatedResult = new BigDecimal(String.valueOf(result)).doubleValue();
    }
  }

  @Override
  public String getEvaluatedResultString() {
    return evaluatedResult.toString();
  }

  @Override
  public DoubleParameter asDoubleParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.DOUBLE;
  }

  @Override
  public ParamDefinition toDefinition() {
    DoubleParamDefinition.DoubleParamDefinitionBuilder<?, ?> builder =
        DoubleParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(new BigDecimal(String.valueOf(evaluatedResult))).build();
  }

  @Override
  public Double getLiteralValue() {
    return getValue().doubleValue();
  }
}
