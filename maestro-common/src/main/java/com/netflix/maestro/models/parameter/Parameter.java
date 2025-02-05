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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.utils.Checks;
import java.util.Map;

/** Parameter interface. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "STRING", value = StringParameter.class),
  @JsonSubTypes.Type(name = "LONG", value = LongParameter.class),
  @JsonSubTypes.Type(name = "DOUBLE", value = DoubleParameter.class),
  @JsonSubTypes.Type(name = "BOOLEAN", value = BooleanParameter.class),
  @JsonSubTypes.Type(name = "STRING_MAP", value = StringMapParameter.class),
  @JsonSubTypes.Type(name = "STRING_ARRAY", value = StringArrayParameter.class),
  @JsonSubTypes.Type(name = "LONG_ARRAY", value = LongArrayParameter.class),
  @JsonSubTypes.Type(name = "DOUBLE_ARRAY", value = DoubleArrayParameter.class),
  @JsonSubTypes.Type(name = "BOOLEAN_ARRAY", value = BooleanArrayParameter.class),
  @JsonSubTypes.Type(name = "MAP", value = MapParameter.class),
  @JsonSubTypes.Type(name = "SIGNAL", value = SignalParameter.class),
})
public interface Parameter {

  /** get parameter name. */
  String getName();

  /** set parameter name. */
  void setName(String name);

  /** get parameter value. */
  <T> T getValue();

  /** get parameter expression. */
  String getExpression();

  /** get parameter type. */
  ParamType getType();

  /** get parameter mode. */
  ParamMode getMode();

  /** get tags. */
  TagList getTags();

  /** get parameter evaluated result. */
  <T> T getEvaluatedResult();

  /** get parameter evaluated result in string format. */
  @JsonIgnore
  String getEvaluatedResultString();

  /** set parameter evaluated result. */
  void setEvaluatedResult(Object result);

  /** get parameter evaluation time. */
  Long getEvaluatedTime();

  /** set parameter evaluation time. */
  void setEvaluatedTime(Long timestamp);

  /** Validation at initialization. */
  void validate();

  /** Get Parameter source. */
  ParamSource getSource();

  /**
   * Convert a parameter to a parameter definition, where its value is set by evaluatedResult and
   * other param attributes (e.g. param mode) will be copied.
   */
  ParamDefinition toDefinition();

  /** true if the parameter is defined as a literal; false if it is a sel expression. */
  @JsonIgnore
  default boolean isLiteral() {
    return getValue() != null;
  }

  /** true if the parameter is evaluated. */
  @JsonIgnore
  default boolean isEvaluated() {
    return getEvaluatedTime() != null;
  }

  /** Get the literal value derived from a parameter's value field. */
  @JsonIgnore
  default Object getLiteralValue() {
    return getValue();
  }

  /** preprocessing the parameters in instances by validating result and setting name. */
  static Map<String, Parameter> preprocessInstanceParams(Map<String, Parameter> params) {
    if (params != null) {
      params.forEach(
          (n, p) -> {
            // allow both null in parameter instance case
            Checks.checkTrue(
                p.getValue() == null || p.getExpression() == null,
                "Param [%s] cannot be defined by both a literal value and an expression",
                n);

            Checks.checkTrue(
                p.getEvaluatedResult() != null && p.getEvaluatedTime() != null,
                "Param [%s] evaluated result and evaluated time should be set in the instance",
                n);
            p.setName(n);
          });
    }
    return params;
  }

  /**
   * get evaluated STRING type (Java String type) param result.
   *
   * @return evaluated result.
   */
  default String asString() {
    return asStringParam().getEvaluatedResult();
  }

  /**
   * get evaluated LONG type (Java Long type) param result.
   *
   * @return evaluated result.
   */
  default Long asLong() {
    return asLongParam().getEvaluatedResult();
  }

  /**
   * get evaluated DOUBLE type (Java Double type) param result.
   *
   * @return evaluated result.
   */
  default Double asDouble() {
    return asDoubleParam().getEvaluatedResult();
  }

  /**
   * get evaluated BOOLEAN type (Java Boolean type) param result.
   *
   * @return evaluated result.
   */
  default Boolean asBoolean() {
    return asBooleanParam().getEvaluatedResult();
  }

  /**
   * get evaluated STRING_MAP type (Java Map<String, String> type) param result.
   *
   * @return evaluated result.
   */
  default Map<String, String> asStringMap() {
    return asStringMapParam().getEvaluatedResult();
  }

  /**
   * get evaluated STRING_ARRAY type (Java String[] type) param result.
   *
   * @return evaluated result.
   */
  default String[] asStringArray() {
    return asStringArrayParam().getEvaluatedResult();
  }

  /**
   * get evaluated INT_ARRAY type (Java long[] type) param result.
   *
   * @return evaluated result.
   */
  default long[] asLongArray() {
    return asLongArrayParam().getEvaluatedResult();
  }

  /**
   * get evaluated DOUBLE_ARRAY type (Java double[] type) param result.
   *
   * @return evaluated result.
   */
  default double[] asDoubleArray() {
    return asDoubleArrayParam().getEvaluatedResult();
  }

  /**
   * get evaluated BOOLEAN_ARRAY type (Java boolean[] type) param result.
   *
   * @return evaluated result.
   */
  default boolean[] asBooleanArray() {
    return asBooleanArrayParam().getEvaluatedResult();
  }

  /**
   * get evaluated MAP type (Java Map<String, Object> type) param result.
   *
   * <p>It supports nested structure and thus parameter values in the map can be another MAP type.
   *
   * @return evaluated result.
   */
  default Map<String, Object> asMap() {
    return asMapParam().getEvaluatedResult();
  }

  /**
   * get STRING type (Java String type) param.
   *
   * @return concrete parameter object.
   */
  default StringParameter asStringParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING", getName(), getType());
  }

  /**
   * get LONG type (Java Long type) param.
   *
   * @return concrete parameter object.
   */
  default LongParameter asLongParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as LONG", getName(), getType());
  }

  /**
   * get DOUBLE type (Java Double type) param.
   *
   * @return concrete parameter object.
   */
  default DoubleParameter asDoubleParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as DOUBLE", getName(), getType());
  }

  /**
   * get BOOLEAN type (Java Boolean type) param.
   *
   * @return concrete parameter object.
   */
  default BooleanParameter asBooleanParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as BOOLEAN", getName(), getType());
  }

  /**
   * get STRING_MAP type (Java Map<String, String> type) param.
   *
   * @return concrete parameter object.
   */
  default StringMapParameter asStringMapParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING_MAP", getName(), getType());
  }

  /**
   * get STRING_ARRAY type (Java String[] type) param.
   *
   * @return concrete parameter object.
   */
  default StringArrayParameter asStringArrayParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING_ARRAY", getName(), getType());
  }

  /**
   * get LONG_ARRAY type (Java long[] type) param.
   *
   * @return concrete parameter object.
   */
  default LongArrayParameter asLongArrayParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as LONG_ARRAY", getName(), getType());
  }

  /**
   * get DOUBLE_ARRAY type (Java double[] type) param.
   *
   * @return concrete parameter object.
   */
  default DoubleArrayParameter asDoubleArrayParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as DOUBLE_ARRAY", getName(), getType());
  }

  /**
   * get BOOLEAN_ARRAY type (Java boolean[] type) param.
   *
   * @return concrete parameter object.
   */
  default BooleanArrayParameter asBooleanArrayParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as BOOLEAN_ARRAY", getName(), getType());
  }

  /**
   * get MAP type (Java Map<String, Parameter> type) param.
   *
   * <p>It supports nested structure and thus parameter values in the map can be another MAP type.
   *
   * @return concrete parameter object.
   */
  default MapParameter asMapParam() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as MAP", getName(), getType());
  }

  /** Return the wrapped parameter inside. By default, return itself. */
  default Parameter unwrap() {
    return this;
  }
}
