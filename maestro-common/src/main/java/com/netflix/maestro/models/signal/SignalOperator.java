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
package com.netflix.maestro.models.signal;

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
  EQUALS_TO("=", 1) {
    @Override
    public boolean apply(long l1, long l2) {
      return l1 == l2;
    }

    @Override
    public boolean apply(String s1, String s2) {
      return s1.compareTo(s2) == 0;
    }
  },
  /** GREATER_THAN. Currently, this operator is only applicable for the {@link Number} types. */
  GREATER_THAN(">", 2) {
    @Override
    public boolean apply(long l1, long l2) {
      return l1 > l2;
    }

    @Override
    public boolean apply(String s1, String s2) {
      return s1.compareTo(s2) > 0;
    }
  },
  /** Currently, this operator is only applicable for the {@link Number} types. */
  GREATER_THAN_EQUALS_TO(">=", 3) {
    @Override
    public boolean apply(long l1, long l2) {
      return l1 >= l2;
    }

    @Override
    public boolean apply(String s1, String s2) {
      return s1.compareTo(s2) >= 0;
    }
  },
  /** LESS_THAN. Currently, this operator is only applicable for the {@link Number} types. */
  LESS_THAN("<", 4) {
    @Override
    public boolean apply(long l1, long l2) {
      return l1 < l2;
    }

    @Override
    public boolean apply(String s1, String s2) {
      return s1.compareTo(s2) < 0;
    }
  },
  /** Currently, this operator is only applicable for the {@link Number} types. */
  LESS_THAN_EQUALS_TO("<=", 5) {
    @Override
    public boolean apply(long l1, long l2) {
      return l1 <= l2;
    }

    @Override
    public boolean apply(String s1, String s2) {
      return s1.compareTo(s2) <= 0;
    }
  };

  private static final Map<String, SignalOperator> OPERATOR_MAP =
      Stream.of(SignalOperator.values())
          .collect(Collectors.toMap(s -> s.operatorCode, Function.identity()));

  public boolean isRangeParam() {
    return order > 1;
  }

  public abstract boolean apply(long l1, long l2);

  /** s1 and s2 should not be null. */
  public abstract boolean apply(String s1, String s2);

  private final String operatorCode;
  private final int order;

  SignalOperator(String operatorCode, int order) {
    this.operatorCode = operatorCode;
    this.order = order;
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
}
