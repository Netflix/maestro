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
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroInternalError;
import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * BOOLEAN_ARRAY Parameter instance.
 *
 * <p>SHOULD NOT mutate the evaluated array data.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
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
@JsonDeserialize(builder = BooleanArrayParameter.BooleanArrayParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class BooleanArrayParameter extends AbstractParameter {
  private final boolean[] value;
  private boolean[] evaluatedResult;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class BooleanArrayParameterBuilderImpl
      extends BooleanArrayParameterBuilder<
          BooleanArrayParameter, BooleanArrayParameterBuilderImpl> {
    @Override
    public BooleanArrayParameter build() {
      BooleanArrayParameter param = new BooleanArrayParameter(this);
      param.validate();
      return param;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setEvaluatedResult(Object result) {
    if (result instanceof boolean[]) {
      this.evaluatedResult = (boolean[]) result;
    } else if (result instanceof List) {
      @SuppressWarnings("unchecked")
      List<Boolean> list = (List<Boolean>) result;
      boolean[] res = new boolean[list.size()];
      int idx = 0;
      for (Boolean item : list) {
        res[idx++] = item;
      }
      this.evaluatedResult = res;
    } else {
      throw new MaestroInternalError(
          "Param [%s] has an invalid evaluated result [%s]", getName(), result);
    }
  }

  @Override
  public String getEvaluatedResultString() {
    return Arrays.toString(evaluatedResult);
  }

  @Override
  public BooleanArrayParameter asBooleanArrayParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.BOOLEAN_ARRAY;
  }

  @Override
  public ParamDefinition toDefinition() {
    BooleanArrayParamDefinition.BooleanArrayParamDefinitionBuilder<?, ?> builder =
        BooleanArrayParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(evaluatedResult).build();
  }
}
