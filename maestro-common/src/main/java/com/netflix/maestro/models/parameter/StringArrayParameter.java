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
import com.netflix.maestro.exceptions.MaestroInternalError;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * STRING_ARRAY Parameter instance.
 *
 * <p>SHOULD NOT mutate the evaluated array data.
 */
@Slf4j
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
@JsonDeserialize(builder = StringArrayParameter.StringArrayParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class StringArrayParameter extends AbstractParameter {
  private final String[] value;
  private String[] evaluatedResult;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class StringArrayParameterBuilderImpl
      extends StringArrayParameterBuilder<StringArrayParameter, StringArrayParameterBuilderImpl> {
    @Override
    public StringArrayParameter build() {
      StringArrayParameter param = new StringArrayParameter(this);
      param.validate();
      return param;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    if (result instanceof String[]) {
      this.evaluatedResult = (String[]) result;
    } else if (result instanceof long[]) {
      this.evaluatedResult =
          Arrays.stream((long[]) result).mapToObj(String::valueOf).toArray(String[]::new);
    } else if (result instanceof double[]) {
      this.evaluatedResult =
          Arrays.stream((double[]) result).mapToObj(String::valueOf).toArray(String[]::new);
    } else if (result instanceof boolean[]) {
      final boolean[] array = (boolean[]) result;
      this.evaluatedResult =
          IntStream.range(0, array.length)
              .mapToObj(idx -> String.valueOf(array[idx]))
              .toArray(String[]::new);
    } else if (result instanceof List) {
      this.evaluatedResult =
          ((List<?>) result).stream().map(String::valueOf).toArray(String[]::new);
    } else {
      throw new MaestroInternalError(
          "Param [%s] has an invalid evaluated result [%s]", getName(), result);
    }
    if (!(result instanceof String[])) {
      LOG.info(
          "Param [{}] is expected to be a String array but is [{}] and will implicitly convert it to a string array.",
          getName(),
          result.getClass());
      this.getMeta()
          .put(
              "info",
              "Implicitly converted the evaluated result to a string array for type "
                  + result.getClass());
    }
  }

  @Override
  public String getEvaluatedResultString() {
    return Arrays.toString(evaluatedResult);
  }

  @Override
  public StringArrayParameter asStringArrayParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.STRING_ARRAY;
  }

  @Override
  public ParamDefinition toDefinition() {
    StringArrayParamDefinition.StringArrayParamDefinitionBuilder<?, ?> builder =
        StringArrayParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder.value(evaluatedResult).build();
  }
}
