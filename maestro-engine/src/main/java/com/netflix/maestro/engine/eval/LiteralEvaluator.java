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
package com.netflix.maestro.engine.eval;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.JsonHelper;
import com.netflix.maestro.utils.MapHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Literal value evaluator with the support of string interpolation. */
@SuppressWarnings("checkstyle:MagicNumber")
@Slf4j
public final class LiteralEvaluator {
  // Matches patterns $$, non-$, or start of string without matching
  private static final String UNESCAPED_MATCHER = "(?<=(?:\\$\\$)|[^$]|^)";

  // Same as UNESCAPED_MATCHER but with an extra $ prefix which is matched
  private static final String ESCAPED_MATCHER = UNESCAPED_MATCHER + "\\$";
  // Captures $$ into a group
  private static final String DOUBLE_DOLLAR_MATCHER = "(\\$\\$)";

  // Matches $var or ${var} as first or second group capturing the variable name without ${} wrapper
  private static final String VARIABLE_MATCHER =
      "\\$(?:\\{([a-zA-Z][\\-_./a-zA-Z0-9]*)}|([a-zA-Z][_a-zA-Z0-9]*))";

  // Matches $var or ${var} as first or second group capturing the entire wrapper with ${} included
  private static final String ESCAPED_VARIABLE_MATCHER =
      "(?:(\\$\\{[a-zA-Z][\\-_\\.\\/a-zA-Z0-9]*\\})|(\\$[a-zA-Z][_a-zA-Z0-9]*))";

  // Forms a regex expression for exactly identifying variables strings $var and ${var} and not
  // escaped variables like $$var and $${var}
  private static final Pattern VARIABLE_REGEX =
      Pattern.compile(UNESCAPED_MATCHER + VARIABLE_MATCHER);

  // Forms a regex expression for identifying variables strings $var and ${var},
  // escaped variable strings like $$var and $${var}, and $$ expressions
  // Each of which need to have a replacement operation performed during parameter binding
  private static final Pattern POTENTIAL_VARIABLE_REGEX =
      Pattern.compile(
          "(?:"
              + UNESCAPED_MATCHER
              + VARIABLE_MATCHER
              + ")|(?:"
              + ESCAPED_MATCHER
              + ESCAPED_VARIABLE_MATCHER
              + ")|"
              + DOUBLE_DOLLAR_MATCHER);

  private static final ObjectMapper MAPPER = JsonHelper.objectMapper();

  private LiteralEvaluator() {}

  /** Parse an input param's literal value with the string interpolation support. */
  public static Object eval(Parameter param, Map<String, Parameter> params) {
    if (!param.isEvaluated()) {
      switch (param.getType()) {
        case STRING:
          return interpolate(param.asStringParam().getValue(), params);
        case STRING_ARRAY:
          return Arrays.stream(param.asStringArrayParam().getValue())
              .map(v -> interpolate(v, params))
              .toArray(String[]::new);
        case STRING_MAP:
          try {
            return param.asStringMapParam().getValue().entrySet().stream()
                .collect(
                    MapHelper.toListMap(Map.Entry::getKey, e -> interpolate(e.getValue(), params)));
          } catch (Exception e) {
            LOG.error(
                "Failed to evaluate literal param: {} due to ",
                param.asStringMapParam().getValue(),
                e);
            throw e;
          }
        default:
          return param.getValue();
      }
    } else {
      return param.getEvaluatedResult();
    }
  }

  private static String interpolate(String value, Map<String, Parameter> params) {
    Matcher m = POTENTIAL_VARIABLE_REGEX.matcher(value);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      Optional<String> result =
          extractParamName(m)
              .flatMap(
                  name -> {
                    if (params.containsKey(name)) {
                      Parameter parameter = params.get(name);
                      if (parameter.isEvaluated()) {
                        return Optional.of(getEvaluatedResultAsString(name, parameter));
                      } else {
                        throw new MaestroInternalError(
                            "Cannot interpolate [%s] as param [%s] is not evaluated yet",
                            value, name);
                      }
                    }
                    // return Optional.empty();
                    throw new MaestroInternalError(
                        "Cannot interpolate [%s] as param [%s] is not found", value, name);
                  });
      if (!result.isPresent()) {
        result = extractEscapedValue(m);
      }
      m.appendReplacement(sb, Matcher.quoteReplacement(result.orElseGet(() -> m.group(0))));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private static String getEvaluatedResultAsString(String paramName, Parameter param) {
    switch (param.getType()) {
      case STRING_ARRAY:
      case DOUBLE_ARRAY:
      case LONG_ARRAY:
      case BOOLEAN_ARRAY:
      case MAP:
      case STRING_MAP:
        try {
          Object result = param.getEvaluatedResult();
          return MAPPER.writeValueAsString(result);
        } catch (JsonProcessingException e) {
          throw new MaestroInternalError(e, "Cannot evaluate [%s] as string due to", paramName);
        }
      default:
        return param.getEvaluatedResultString();
    }
  }

  /** Get reference parameter names from an input param with the string interpolation support. */
  public static Set<String> getReferencedParamNames(Parameter param) {
    switch (param.getType()) {
      case STRING:
        return getReferencedParamNames(param.asStringParam().getValue());
      case STRING_ARRAY:
        return Arrays.stream(param.asStringArrayParam().getValue())
            .map(LiteralEvaluator::getReferencedParamNames)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
      case STRING_MAP:
        return param.asStringMapParam().getValue().values().stream()
            .map(LiteralEvaluator::getReferencedParamNames)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
      default:
        return Collections.emptySet();
    }
  }

  private static Set<String> getReferencedParamNames(String value) {
    Matcher m = VARIABLE_REGEX.matcher(value);
    Set<String> paramNames = new LinkedHashSet<>();
    while (m.find()) {
      extractParamName(m).ifPresent(paramNames::add);
    }
    return paramNames;
  }

  /*
   * Matches for variable references that match the following groups, or None:
   * - group(1) is the ${(variable)} style matcher
   * - group(2) is the $(variable) style matcher
   */
  private static Optional<String> extractParamName(Matcher m) {
    Optional<String> result = maybeGroup(m, 1);
    if (!result.isPresent()) {
      result = maybeGroup(m, 2);
    }
    return result;
  }

  /*
   * Matches for non-variable references that match the following groups, or None:
   * - group(3) is the $(${variable}) style matcher, which is the same as the group(1) matcher for an escaped variable
   * - group(4) is the $($variable) style matcher, which is the same as the group(2) matcher for an escaped variable
   * - group(5) is the unassociated ($$) style matcher
   */
  private static Optional<String> extractEscapedValue(Matcher m) {
    Optional<String> result = maybeGroup(m, 3);
    if (!result.isPresent()) {
      result = maybeGroup(m, 4);
      if (!result.isPresent()) {
        result = maybeGroup(m, 5).map(v -> "$");
      }
    }
    return result;
  }

  private static Optional<String> maybeGroup(Matcher m, int groupIndex) {
    if (m.groupCount() >= groupIndex) {
      return Optional.ofNullable(m.group(groupIndex));
    } else {
      return Optional.empty();
    }
  }
}
