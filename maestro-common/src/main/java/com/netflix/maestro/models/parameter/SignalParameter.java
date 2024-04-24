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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Payload for the signal dependency parameter. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"parameter", "operator"},
    alphabetic = true)
@JsonDeserialize(builder = SignalParameter.SignalParameterBuilder.class)
@Builder
@ToString
@EqualsAndHashCode
public class SignalParameter implements Parameter {
  private Parameter parameter;
  private SignalOperator operator;

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

  @JsonIgnore
  @Override
  public ParamMode getMode() {
    return parameter.getMode();
  }

  @JsonIgnore
  @Override
  public TagList getTags() {
    return parameter.getTags();
  }

  @JsonIgnore
  @Override
  public <T> T getEvaluatedResult() {
    return parameter.getEvaluatedResult();
  }

  @JsonIgnore
  @Override
  public String getEvaluatedResultString() {
    return parameter.getEvaluatedResultString();
  }

  @Override
  public void setEvaluatedResult(Object result) {
    parameter.setEvaluatedResult(result);
  }

  @JsonIgnore
  @Override
  public Long getEvaluatedTime() {
    return parameter.getEvaluatedTime();
  }

  @Override
  public void setEvaluatedTime(Long timestamp) {
    parameter.setEvaluatedTime(timestamp);
  }

  @Override
  public ParamSource getSource() {
    return parameter.getSource();
  }

  @Override
  public void validate() {
    parameter.validate();
  }

  public Parameter getParameter() {
    return this.parameter;
  }

  public SignalOperator getOperator() {
    return this.operator;
  }

  @Override
  public ParamDefinition toDefinition() {
    return SignalParamDefinition.builder()
        .operator(operator)
        .parameter(parameter.toDefinition())
        .build();
  }

  /** Builder class for SignalParameter. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class SignalParameterBuilder {}

  @JsonIgnore
  @Override
  public Parameter unwrap() {
    return this.parameter;
  }
}
