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
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.utils.ParamHelper;
import java.math.BigDecimal;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * DOUBLE_ARRAY Parameter instance.
 *
 * <p>SHOULD NOT mutate the evaluated array data.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
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
@JsonDeserialize(builder = DoubleArrayParameter.DoubleArrayParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class DoubleArrayParameter extends AbstractParameter {
  private final BigDecimal[] value;
  private double[] evaluatedResult;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class DoubleArrayParameterBuilderImpl
      extends DoubleArrayParameterBuilder<DoubleArrayParameter, DoubleArrayParameterBuilderImpl> {
    @Override
    public DoubleArrayParameter build() {
      DoubleArrayParameter param = new DoubleArrayParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    this.evaluatedResult = ParamHelper.toDoubleArray(getName(), result);
  }

  @Override
  public String getEvaluatedResultString() {
    return Arrays.toString(evaluatedResult);
  }

  @Override
  public DoubleArrayParameter asDoubleArrayParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.DOUBLE_ARRAY;
  }

  @Override
  public ParamDefinition toDefinition() {
    DoubleArrayParamDefinition.DoubleArrayParamDefinitionBuilder<?, ?> builder =
        DoubleArrayParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(ParamHelper.toDecimalArray(getName(), evaluatedResult)).build();
  }

  @Override
  public double[] getLiteralValue() {
    return ParamHelper.toDoubleArray(getName(), getValue());
  }
}
