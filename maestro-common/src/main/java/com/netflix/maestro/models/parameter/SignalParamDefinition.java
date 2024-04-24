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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.definition.TagList;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Payload for the signal dependency parameter definition. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"parameter", "operator"},
    alphabetic = true)
@JsonDeserialize(builder = SignalParamDefinition.SignalParamDefinitionBuilder.class)
@Builder
@ToString
@EqualsAndHashCode
public class SignalParamDefinition implements ParamDefinition {
  private final ParamDefinition parameter;
  private final SignalOperator operator;

  @JsonIgnore
  @Override
  public String getName() {
    return parameter.getName();
  }

  @Override
  public void setName(String name) {
    parameter.setName(name);
  }

  @JsonIgnore
  @Override
  public <T> T getValue() {
    return parameter.getValue();
  }

  @JsonIgnore
  @Override
  public String getExpression() {
    return parameter.getExpression();
  }

  @Override
  public ParamType getType() {
    return ParamType.SIGNAL;
  }

  @Override
  public void validate() {
    parameter.validate();
  }

  public ParamDefinition getParameter() {
    return this.parameter;
  }

  public SignalOperator getOperator() {
    return this.operator;
  }

  @Override
  public Parameter toParameter() {
    return SignalParameter.builder().operator(operator).parameter(parameter.toParameter()).build();
  }

  @JsonIgnore
  @Override
  public ParamMode getMode() {
    return parameter.getMode();
  }

  @JsonIgnore
  @Override
  public ParamSource getSource() {
    return parameter.getSource();
  }

  @JsonIgnore
  @Override
  public InternalParamMode getInternalMode() {
    return parameter.getInternalMode();
  }

  @JsonIgnore
  @Override
  public TagList getTags() {
    return parameter.getTags();
  }

  @Override
  public ParamDefinition copyAndUpdate(
      Object updatedValue,
      String expression,
      ParamMode mode,
      Map<String, Object> meta,
      TagList tagList,
      ParamValidator validator) {
    return parameter.copyAndUpdate(updatedValue, expression, mode, meta, tagList, validator);
  }

  /** Builder class for SignalParamDefinition. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class SignalParamDefinitionBuilder {}
}
