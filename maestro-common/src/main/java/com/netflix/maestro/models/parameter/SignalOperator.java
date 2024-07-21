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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;

/** Operators applied to the signal param values. */
@Getter
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum SignalOperator {
  /** EQUALS_TO. */
  EQUALS_TO(Constants.EQUALS),
  /** LESS_THAN. Currently, this operator is only applicable for the {@link Number} types. */
  LESS_THAN("<"),
  /** GREATER_THAN. Currently, this operator is only applicable for the {@link Number} types. */
  GREATER_THAN(">"),
  /** Currently, this operator is only applicable for the {@link Number} types. */
  GREATER_THAN_EQUALS_TO(">="),
  /** Currently, this operator is only applicable for the {@link Number} types. */
  LESS_THAN_EQUALS_TO("<=");

  private static final Map<String, SignalOperator> OPERATOR_MAP =
      Stream.of(SignalOperator.values())
          .collect(Collectors.toMap(s -> s.operatorCode, Function.identity()));

  public boolean isRangeParam() {
    return !operatorCode.equals(Constants.EQUALS);
  }

  private final String operatorCode;

  SignalOperator(String operatorCode) {
    this.operatorCode = operatorCode;
  }

  @Override
  @JsonValue
  public String toString() {
    return operatorCode;
  }

  /**
   * Helper method to create Operator from operatorCode.
   *
   * @param operatorCode operator code
   * @return operator object
   */
  @JsonCreator
  public static SignalOperator create(String operatorCode) {
    return Optional.ofNullable(OPERATOR_MAP.get(operatorCode))
        .orElseThrow(() -> new IllegalArgumentException("Invalid operator code: " + operatorCode));
  }

  private static final class Constants {
    private static final String EQUALS = "=";
  }
}
