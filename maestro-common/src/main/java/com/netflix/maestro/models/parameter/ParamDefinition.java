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
import com.netflix.maestro.utils.ParamHelper;
import java.math.BigDecimal;
import java.util.Map;

/** Parameter definition interface. */
@SuppressWarnings({"PMD.UseVarargs"})
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "STRING", value = StringParamDefinition.class),
  @JsonSubTypes.Type(name = "LONG", value = LongParamDefinition.class),
  @JsonSubTypes.Type(name = "DOUBLE", value = DoubleParamDefinition.class),
  @JsonSubTypes.Type(name = "BOOLEAN", value = BooleanParamDefinition.class),
  @JsonSubTypes.Type(name = "STRING_MAP", value = StringMapParamDefinition.class),
  @JsonSubTypes.Type(name = "STRING_ARRAY", value = StringArrayParamDefinition.class),
  @JsonSubTypes.Type(name = "LONG_ARRAY", value = LongArrayParamDefinition.class),
  @JsonSubTypes.Type(name = "DOUBLE_ARRAY", value = DoubleArrayParamDefinition.class),
  @JsonSubTypes.Type(name = "BOOLEAN_ARRAY", value = BooleanArrayParamDefinition.class),
  @JsonSubTypes.Type(name = "MAP", value = MapParamDefinition.class),
  @JsonSubTypes.Type(name = "SIGNAL", value = SignalParamDefinition.class)
})
public interface ParamDefinition {

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

  /** Validation at initialization. */
  void validate();

  /** Convert a parameter definition to an un-evaluated parameter instance. */
  Parameter toParameter();

  /** true if the parameter is defined as a literal; false if it is a sel expression. */
  @JsonIgnore
  default boolean isLiteral() {
    return getValue() != null;
  }

  /** get parameter mode. */
  ParamMode getMode();

  /** Get internal param mode. */
  InternalParamMode getInternalMode();

  /** Get param source. */
  ParamSource getSource();

  /** get tags. */
  TagList getTags();

  /** Create a new param definition from copy and passed arguments. */
  ParamDefinition copyAndUpdate(
      Object updatedValue,
      String expression,
      ParamMode mode,
      Map<String, Object> meta,
      TagList tagList,
      ParamValidator validator);

  /** preprocessing the parameters in definition by validating value and setting name. */
  static Map<String, ParamDefinition> preprocessDefinitionParams(
      Map<String, ParamDefinition> params) {
    if (params != null) {
      params.forEach(
          (n, p) -> {
            Checks.checkTrue(
                (p.getValue() != null && p.getExpression() == null)
                    || (p.getValue() == null && p.getExpression() != null),
                "Param [%s] must be defined with either a literal value or an expression for parameter",
                n);
            p.setName(n);
          });
    }
    return params;
  }

  /** Build parameter for boolean array. */
  static BooleanArrayParamDefinition buildParamDefinition(String key, boolean[] values) {
    return BooleanArrayParamDefinition.builder().name(key).value(values).build();
  }

  /** Build parameter for boolean. */
  static BooleanParamDefinition buildParamDefinition(String key, boolean value) {
    return BooleanParamDefinition.builder().name(key).value(value).build();
  }

  /** Build parameter for double array. */
  static DoubleArrayParamDefinition buildParamDefinition(String key, double[] values) {
    return DoubleArrayParamDefinition.builder()
        .name(key)
        .value(ParamHelper.toDecimalArray(key, values))
        .build();
  }

  /** Build parameter for double. */
  static DoubleParamDefinition buildParamDefinition(String key, double value) {
    return DoubleParamDefinition.builder()
        .name(key)
        .value(new BigDecimal(String.valueOf(value)))
        .build();
  }

  /** Build parameter for long array. */
  static LongArrayParamDefinition buildParamDefinition(String key, long[] values) {
    return LongArrayParamDefinition.builder().name(key).value(values).build();
  }

  /** Build parameter for long. */
  static LongParamDefinition buildParamDefinition(String key, long value) {
    return LongParamDefinition.builder().name(key).value(value).build();
  }

  /** Build parameter for String array. */
  static StringArrayParamDefinition buildParamDefinition(String key, String[] values) {
    return StringArrayParamDefinition.builder().name(key).value(values).build();
  }

  /** Build parameter for StringMap. */
  static StringMapParamDefinition buildParamDefinition(String key, Map<String, String> values) {
    return StringMapParamDefinition.builder().name(key).value(values).build();
  }

  /** Build parameter for String. */
  static StringParamDefinition buildParamDefinition(String key, String value) {
    return StringParamDefinition.builder().name(key).value(value).build();
  }

  /**
   * get STRING type (Java String type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default StringParamDefinition asStringParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING", getName(), getType());
  }

  /**
   * get LONG type (Java Long type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default LongParamDefinition asLongParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as LONG", getName(), getType());
  }

  /**
   * get DOUBLE type (Java Double type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default DoubleParamDefinition asDoubleParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as DOUBLE", getName(), getType());
  }

  /**
   * get BOOLEAN type (Java Boolean type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default BooleanParamDefinition asBooleanParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as BOOLEAN", getName(), getType());
  }

  /**
   * get STRING_MAP type (Java Map<String, String> type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default StringMapParamDefinition asStringMapParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING_MAP", getName(), getType());
  }

  /**
   * get STRING_ARRAY type (Java String[] type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default StringArrayParamDefinition asStringArrayParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as STRING_ARRAY", getName(), getType());
  }

  /**
   * get LONG_ARRAY type (Java long[] type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default LongArrayParamDefinition asLongArrayParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as LONG_ARRAY", getName(), getType());
  }

  /**
   * get DOUBLE_ARRAY type (Java double[] type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default DoubleArrayParamDefinition asDoubleArrayParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as DOUBLE_ARRAY", getName(), getType());
  }

  /**
   * get BOOLEAN_ARRAY type (Java boolean[] type) paramDefinition.
   *
   * @return concrete parameter definition object.
   */
  default BooleanArrayParamDefinition asBooleanArrayParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as BOOLEAN_ARRAY", getName(), getType());
  }

  /**
   * get MAP type (Java Map<String, ParamDefinition> type) paramDefinition.
   *
   * <p>It supports nested structure and thus parameter values in the map can be another MAP type.
   *
   * @return concrete parameter definition object.
   */
  default MapParamDefinition asMapParamDef() {
    throw new MaestroInternalError(
        "Param [%s] is a [%s] type and cannot be used as MAP", getName(), getType());
  }
}
