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
import com.netflix.maestro.models.definition.TagList;
import jakarta.validation.Valid;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Map parameter definition to support Java Map<String, Object> type.
 *
 * <p>In MAP parameter, if its value must be specific, the value should be a Map<String,
 * ParamDefinition> object. Each entry in the map should be another parameter. It supports nested
 * structure and thus parameter values in the map can be another MAP type.
 *
 * <p>If its expression is specific, the expression is a SEL string, which returns a Map<String,
 * Object> object after the evaluation. In this case, SEL evaluator will ensure the data type is
 * compatible with Maestro.
 *
 * <p>SHOULD NOT mutate the returned string map data.
 */
@SuppressWarnings("unchecked")
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"name", "value", "expression", "type", "validator", "tags", "mode"},
    alphabetic = true)
@JsonDeserialize(builder = MapParamDefinition.MapParamDefinitionBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class MapParamDefinition extends AbstractParamDefinition {
  @Valid private final Map<String, ParamDefinition> value;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class MapParamDefinitionBuilderImpl
      extends MapParamDefinitionBuilder<MapParamDefinition, MapParamDefinitionBuilderImpl> {
    @Override
    public MapParamDefinition build() {
      if (super.value != null) { // set the param name
        super.value.forEach((n, p) -> p.setName(n));
      }
      MapParamDefinition param = new MapParamDefinition(this);
      param.validate();
      return param;
    }
  }

  @Override
  public MapParamDefinition asMapParamDef() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.MAP;
  }

  @Override
  public Parameter toParameter() {
    return MapParameter.builder()
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
        .value((Map<String, ParamDefinition>) updatedValue)
        .expression(expression)
        .validator(validator)
        .tags(tagList)
        .mode(mode)
        .meta(meta)
        .build();
  }
}
