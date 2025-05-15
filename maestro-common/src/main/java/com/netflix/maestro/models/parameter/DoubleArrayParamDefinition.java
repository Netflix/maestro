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
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.utils.ParamHelper;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * DOUBLE_ARRAY Parameter definition.
 *
 * <p>SHOULD NOT mutate the returned array data.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"name", "value", "expression", "type", "validator", "tags", "mode"},
    alphabetic = true)
@JsonDeserialize(builder = DoubleArrayParamDefinition.DoubleArrayParamDefinitionBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class DoubleArrayParamDefinition extends AbstractParamDefinition {
  private final BigDecimal[] value;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class DoubleArrayParamDefinitionBuilderImpl
      extends DoubleArrayParamDefinitionBuilder<
          DoubleArrayParamDefinition, DoubleArrayParamDefinitionBuilderImpl> {
    @Override
    public DoubleArrayParamDefinition build() {
      DoubleArrayParamDefinition param = new DoubleArrayParamDefinition(this);
      param.validate();
      return param;
    }
  }

  @Override
  public ParamType getType() {
    return ParamType.DOUBLE_ARRAY;
  }

  @Override
  public DoubleArrayParamDefinition asDoubleArrayParamDef() {
    return this;
  }

  @Override
  public Parameter toParameter() {
    return DoubleArrayParameter.builder()
        .name(getName())
        .value(getValue())
        .expression(getExpression())
        .validator(getValidator())
        .tags(getTags())
        .mode(getMode())
        .meta(getMeta())
        .build();
  }

  @Override
  public ParamDefinition copyAndUpdate(
      Object updatedValue,
      String expression,
      ParamMode mode,
      Map<String, Object> meta,
      TagList tagList,
      ParamValidator validator) {
    return toBuilder()
        .value(
            Optional.ofNullable(updatedValue)
                .map(val -> ParamHelper.toDecimalArray(getName(), updatedValue))
                .orElse(null))
        .expression(expression)
        .validator(validator)
        .tags(tagList)
        .mode(mode)
        .meta(meta)
        .build();
  }
}
