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
package com.netflix.maestro.models.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Exception details. */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder(value = {"status", "message", "errors", "retryable"})
@Getter
@Builder
@ToString
@JsonDeserialize(builder = Details.DetailsBuilder.class)
@EqualsAndHashCode
public class Details {
  private static final int MAX_STACK_TRACE_IN_ERRORS = 3;
  private final MaestroRuntimeException.Code status;
  private final String message;
  private final List<String> errors;
  private final Throwable cause;
  private final boolean retryable;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class DetailsBuilder {}

  /** static creator. */
  public static Details create(String template, Object... args) {
    return Details.builder()
        .status(MaestroRuntimeException.Code.INTERNAL_ERROR)
        .message(String.format(template, args))
        .build();
  }

  /** static creator. */
  public static Details create(Throwable cause, boolean retryable, String msg) {
    List<String> errors = new ArrayList<>();
    errors.add(cause.getClass().getSimpleName() + ": " + cause.getMessage());
    if (!retryable) {
      StackTraceElement[] elements = cause.getStackTrace();
      if (elements != null && elements.length > 0) {
        int loopNum = Math.min(MAX_STACK_TRACE_IN_ERRORS, elements.length);
        for (int i = 0; i < loopNum; ++i) {
          errors.add("  at " + cause.getStackTrace()[i].toString());
        }
      }
    }
    if (cause instanceof MaestroInternalError) {
      MaestroInternalError internalError = ((MaestroInternalError) cause);
      if (internalError.getDetails().getErrors() != null) {
        errors.addAll(internalError.getDetails().getErrors());
      }
    } else if (!Objects.isNull(cause.getCause())) {
      Throwable nestedCause = cause.getCause();
      errors.add(nestedCause.getMessage());
      if (nestedCause instanceof MaestroInternalError) {
        MaestroInternalError internalError = ((MaestroInternalError) nestedCause);
        if (internalError.getDetails().getErrors() != null) {
          errors.addAll(internalError.getDetails().getErrors());
        }
      } else if (!Objects.isNull(nestedCause.getCause())) {
        errors.add(nestedCause.getCause().getMessage());
      }
    }

    return Details.builder()
        .status(MaestroRuntimeException.Code.INTERNAL_ERROR)
        .message(msg)
        .errors(errors)
        .cause(cause)
        .retryable(retryable)
        .build();
  }
}
