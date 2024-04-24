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
package com.netflix.maestro.utils;

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.parameter.BooleanArrayParameter;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.DoubleArrayParameter;
import com.netflix.maestro.models.parameter.DoubleParameter;
import com.netflix.maestro.models.parameter.LongArrayParameter;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Param helper utility class. */
public final class ParamHelper {
  private static final int ERROR_MESSAGE_SIZE_LIMIT = 1000;

  private ParamHelper() {}

  /** Create a placeholder parameter with derived param type based on the evaluated result. */
  public static Parameter deriveTypedParameter(
      String name,
      String expression,
      Object evaluatedResult,
      TagList tags,
      ParamMode paramMode,
      Map<String, Object> paramMeta) {
    if (evaluatedResult instanceof String) {
      return StringParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof Long) {
      return LongParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof Double) {
      return DoubleParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof Boolean) {
      return BooleanParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof Map) {
      return MapParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof List) {
      List<?> list = (List<?>) evaluatedResult;
      if (!list.isEmpty()) {
        Object item = list.get(0);
        if (item instanceof String) {
          return StringArrayParameter.builder()
              .name(name)
              .expression(expression)
              .tags(tags)
              .mode(paramMode)
              .meta(paramMeta)
              .build();
        } else if (item instanceof Long) {
          return LongArrayParameter.builder()
              .name(name)
              .expression(expression)
              .tags(tags)
              .mode(paramMode)
              .meta(paramMeta)
              .build();
        } else if (item instanceof Double || item instanceof BigDecimal) {
          return DoubleArrayParameter.builder()
              .name(name)
              .expression(expression)
              .tags(tags)
              .mode(paramMode)
              .meta(paramMeta)
              .build();
        } else if (item instanceof Boolean) {
          return BooleanArrayParameter.builder()
              .name(name)
              .expression(expression)
              .tags(tags)
              .mode(paramMode)
              .meta(paramMeta)
              .build();
        }
      }
    } else if (evaluatedResult instanceof String[]) {
      return StringArrayParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof long[]) {
      return LongArrayParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof double[]) {
      return DoubleArrayParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    } else if (evaluatedResult instanceof boolean[]) {
      return BooleanArrayParameter.builder()
          .name(name)
          .expression(expression)
          .tags(tags)
          .mode(paramMode)
          .meta(paramMeta)
          .build();
    }
    throw new MaestroInternalError(
        "Param [%s] type [%s] is not supported.", name, evaluatedResult.getClass().getName());
  }

  /**
   * Convert an object (might be double[], BigDecimal[], List, etc.) into a BigDecimal array for a
   * param with a given name.
   *
   * @param name name for identification in the error message
   * @param value value can be transformed to BigDecimal[]
   * @return decimal array
   */
  public static BigDecimal[] toDecimalArray(String name, Object value) {
    try {
      if (value instanceof BigDecimal[]) {
        return (BigDecimal[]) value;
      } else if (value instanceof double[]) {
        return Arrays.stream((double[]) value)
            .mapToObj(val -> new BigDecimal(String.valueOf(val)))
            .toArray(BigDecimal[]::new);
      } else if (value instanceof List) {
        return ((List<?>) value)
            .stream().map(d -> new BigDecimal(String.valueOf(d))).toArray(BigDecimal[]::new);
      } else {
        throw new MaestroInternalError(
            "Cannot cast value [%s] into a BigDecimal array for param [%s]",
            toTruncateString(value), name);
      }
    } catch (NumberFormatException nfe) {
      throw new MaestroInternalError(
          nfe, "Invalid number format for value: %s for param [%s]", toTruncateString(value), name);
    }
  }

  /**
   * Convert an object (might be double[], BigDecimal[], List, etc.) into a double array for a param
   * with a given name.
   *
   * @param name name for identification in the error message
   * @param value value can be transformed to double[]
   * @return double array
   */
  public static double[] toDoubleArray(String name, Object value) {
    try {
      if (value instanceof BigDecimal[]) {
        return Arrays.stream((BigDecimal[]) value).mapToDouble(BigDecimal::doubleValue).toArray();
      } else if (value instanceof double[]) {
        return (double[]) value;
      } else if (value instanceof List) {
        return ((List<?>) value)
            .stream().mapToDouble(d -> new BigDecimal(String.valueOf(d)).doubleValue()).toArray();
      } else {
        throw new MaestroInternalError(
            "Param [%s] has an invalid evaluated result [%s]", name, toTruncateString(value));
      }
    } catch (NumberFormatException nfe) {
      throw new MaestroInternalError(
          nfe,
          "Invalid number format for evaluated result: %s for param [%s]",
          toTruncateString(value),
          name);
    }
  }

  private static String toTruncateString(Object input) {
    String msg = String.valueOf(input);
    if (msg != null && msg.length() > ERROR_MESSAGE_SIZE_LIMIT) {
      return msg.substring(0, ERROR_MESSAGE_SIZE_LIMIT);
    }
    return msg;
  }
}
