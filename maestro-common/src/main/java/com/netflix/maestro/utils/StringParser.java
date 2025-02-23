/*
 * Copyright 2025 Netflix, Inc.
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

import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.function.Function;

/** Utility class to parse a parameterized string. */
public final class StringParser {
  /** Private constructor for utility class. */
  private StringParser() {}

  private static final String PARAM_NAME = "maestro_parsable_str";

  /**
   * Parses the parameters within the string.
   *
   * @param value the string value to parse
   * @param paramParser the function to parse the parameter
   * @return the parsed string value
   */
  public static String parseWithParam(
      String value, Function<ParamDefinition, Parameter> paramParser) {
    ParamDefinition paramDef = ParamDefinition.buildParamDefinition(PARAM_NAME, value);
    Parameter param = paramParser.apply(paramDef);
    return param == null ? value : param.asString();
  }
}
