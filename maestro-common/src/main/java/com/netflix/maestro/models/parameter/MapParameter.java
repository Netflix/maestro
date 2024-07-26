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
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.utils.ParamHelper;
import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * Map parameter to support Java Map<String, Object> type param value.
 *
 * <p>In MAP parameter, if its value must be specific, the value should be a Map<String,
 * ParamDefinition> object. Each entry in the map should be another parameter. It supports nested
 * structure and thus parameter values in the map can be another MAP type.
 *
 * <p>If its expression is specific, the expression is a SEL string, which returns a Map<String,
 * Object> object after the evaluation. In this case, SEL evaluator will ensure the data type is
 * compatible with Maestro.
 *
 * <p>SHOULD NOT mutate the evaluated string map data.
 */
@SuppressWarnings("unchecked")
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
@JsonDeserialize(builder = MapParameter.MapParameterBuilderImpl.class)
@Getter(onMethod = @__({@Override}))
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public final class MapParameter extends AbstractParameter {
  @Valid private final Map<String, ParamDefinition> value;

  private Map<String, Object> evaluatedResult;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  static final class MapParameterBuilderImpl
      extends MapParameterBuilder<MapParameter, MapParameterBuilderImpl> {
    @Override
    public MapParameter build() {
      if (super.value != null) { // set the param name
        super.value.forEach((n, p) -> p.setName(n));
      }
      MapParameter param = new MapParameter(this);

      /**
       * As the {@link MapParameter#evaluatedResult} is a Map<String, Object> jackson is unable
       * decodes the Object to ArrayList if the encoded object even when the encoded Object was
       * Array. Thus casting it to arrays.
       */
      if (param.evaluatedResult != null && !param.evaluatedResult.isEmpty()) {
        param.evaluatedResult =
            castValuesWithListToArray(param.getName(), param.value, param.evaluatedResult);
      }
      param.validate();
      return param;
    }

    /**
     * Recursively casts the List values in the provided map to relevant Array types. - If type is
     * known over the param definition, use the type from the definition. - If value of param
     * definition is absent, derive it from value if possible.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private Map<String, Object> castValuesWithListToArray(
        String name, Map<String, ParamDefinition> mapParamDef, Map<String, Object> mapToCast) {
      Map<String, Object> result = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : mapToCast.entrySet()) {
        Object value = entry.getValue();
        Object newValue = value;
        ParamDefinition paramDef = mapParamDef == null ? null : mapParamDef.get(entry.getKey());
        if (value instanceof Map) {
          if (paramDef != null) {
            if (paramDef.getType() == ParamType.MAP) {
              newValue =
                  castValuesWithListToArray(
                      name, paramDef.asMapParamDef().getValue(), (Map<String, Object>) value);
            }
          } else {
            newValue = castValuesWithListToArray(name, null, (Map<String, Object>) value);
          }
        } else if (value instanceof List<?>) {
          final List<?> listValue = (List<?>) value;
          if (!(listValue).isEmpty()) {
            Object firstValue = listValue.get(0);
            if (firstValue instanceof Long) {
              long[] arr = ((List<Long>) value).stream().mapToLong(Long::longValue).toArray();
              newValue = arr;
            } else if (firstValue instanceof Double) {
              double[] arr =
                  ((List<Double>) value).stream().mapToDouble(Double::doubleValue).toArray();
              newValue = arr;
            } else if (firstValue instanceof BigDecimal) {
              double[] arr =
                  ((List<BigDecimal>) value)
                      .stream().mapToDouble(BigDecimal::doubleValue).toArray();
              newValue = arr;
            } else if (firstValue instanceof Boolean) {
              boolean[] arr = new boolean[((List<?>) value).size()];
              for (int i = 0; i < arr.length; i++) {
                arr[i] = ((Boolean) ((List<?>) value).get(i)).booleanValue();
              }
              newValue = arr;
            } else if (firstValue instanceof String) {
              String[] arr = ((List<String>) value).stream().toArray(String[]::new);
              newValue = arr;
            } else {
              throw new MaestroInternalError(
                  "MapParam [%s] param [%s]  has an invalid type List as value [%s]",
                  name, entry.getKey(), value.getClass().getName());
            }
          } else if (paramDef != null) { // empty list case
            if (paramDef.getType() == ParamType.LONG_ARRAY) {
              newValue = new long[0];
            } else if (paramDef.getType() == ParamType.DOUBLE_ARRAY) {
              newValue = new double[0];
            } else if (paramDef.getType() == ParamType.BOOLEAN_ARRAY) {
              newValue = new boolean[0];
            } else if (paramDef.getType() == ParamType.STRING_ARRAY) {
              newValue = new String[0];
            }
          }
        }
        result.put(entry.getKey(), newValue);
      }
      return result;
    }
  }

  @Override
  public void setEvaluatedResult(Object result) {
    this.evaluatedResult = (Map<String, Object>) result;
  }

  @Override
  public String getEvaluatedResultString() {
    throw new UnsupportedOperationException(
        "Map Parameter does not support getEvaluatedResultString.");
  }

  @Override
  public MapParameter asMapParam() {
    return this;
  }

  @Override
  public ParamType getType() {
    return ParamType.MAP;
  }

  @Override
  public ParamDefinition toDefinition() {
    MapParamDefinition.MapParamDefinitionBuilder<?, ?> builder = MapParamDefinition.builder();
    if (isImmutableToDefinitionWithoutValue(builder)) {
      return builder.value(getValue()).expression(getExpression()).build();
    }
    return builder
        .value(
            evaluatedResult.keySet().stream()
                .collect(
                    MapHelper.toListMap(
                        Function.identity(), e -> getEvaluatedParam(e).toDefinition())))
        .build();
  }

  /** get the evaluated result from a parameter inside the map. */
  public <T> T getEvaluatedResultForParam(String paramName) {
    return getEvaluatedParam(paramName).getEvaluatedResult();
  }

  /** get evaluated parameter inside the map. */
  public Parameter getEvaluatedParam(String paramName) {
    if (isEvaluated()) {
      Object result =
          Checks.notNull(
              evaluatedResult.get(paramName),
              "Cannot find param name key [%s] in the map param [%s]",
              paramName,
              getName());
      Parameter param;
      if (isLiteral()) { // assemble the parameter
        param = value.get(paramName).toParameter();
      } else { // use the whole sel expression as the param expression
        param =
            ParamHelper.deriveTypedParameter(
                paramName, getExpression(), result, getTags(), getMode(), getMeta());
      }
      param.setEvaluatedResult(result);
      param.setEvaluatedTime(getEvaluatedTime());
      return param;
    }
    throw new MaestroInternalError(
        "Param [{}] is not evaluated and cannot call getEvaluatedParam()", getName());
  }

  /** Check if parameter is defined. * */
  public boolean containsParam(String key) {
    return getValue().containsKey(key);
  }

  /**
   * Returns the keyset of the underlying evaluatedResult maps.
   *
   * @return set of param names
   */
  @JsonIgnore
  public Set<String> getParamNames() {
    return evaluatedResult.keySet();
  }
}
