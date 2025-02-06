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

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.function.Function;

/** Utility class to parse retry policy. * */
public final class RetryPolicyParser {
  private static final String PARAM_NAME = "retries_str";
  private static final String BACKOFF_ERROR_RETRY_BACKOFF_IN_SECS =
      "backoff.error_retry_backoff_in_secs";
  private static final String BACKOFF_PLATFORM_RETRY_BACKOFF_IN_SECS =
      "backoff.platform_retry_backoff_in_secs";
  private static final String BACKOFF_TIMEOUT_RETRY_BACKOFF_IN_SECS =
      "backoff.timeout_retry_backoff_in_secs";
  private static final String BACKOFF_ERROR_RETRY_EXPONENT = "backoff.error_retry_exponent";
  private static final String BACKOFF_ERROR_RETRY_LIMIT_IN_SECS =
      "backoff.error_retry_limit_in_secs";
  private static final String BACKOFF_PLATFORM_RETRY_EXPONENT = "backoff.platform_retry_exponent";
  private static final String BACKOFF_PLATFORM_RETRY_LIMIT_IN_SECS =
      "backoff.platform_retry_limit_in_secs";
  private static final String BACKOFF_TIMEOUT_RETRY_EXPONENT = "backoff.timeout_retry_exponent";
  private static final String BACKOFF_TIMEOUT_RETRY_LIMIT_IN_SECS =
      "backoff.timeout_retry_limit_in_secs";

  /** Private constructor for utility class. */
  private RetryPolicyParser() {}

  public static RetryPolicy getParsedRetryPolicy(
      RetryPolicy retryPolicy, Function<ParamDefinition, Parameter> paramParser) {
    return retryPolicy.toBuilder()
        .errorRetryLimit(
            getParsedRetryNumber(
                retryPolicy.getErrorRetryLimit(), paramParser, "error_retry_limit"))
        .platformRetryLimit(
            getParsedRetryNumber(
                retryPolicy.getPlatformRetryLimit(), paramParser, "platform_retry_limit"))
        .timeoutRetryLimit(
            getParsedRetryNumber(
                retryPolicy.getTimeoutRetryLimit(), paramParser, "timeout_retry_limit"))
        .backoff(getParsedBackoff(retryPolicy.getBackoff(), paramParser))
        .build();
  }

  private static ParsableLong getParsedRetryNumber(
      @Nullable ParsableLong retryNumber,
      Function<ParamDefinition, Parameter> paramParser,
      String path) {
    return parseParsableLong(retryNumber, paramParser, Constants.MAX_RETRY_LIMIT, path);
  }

  private static ParsableLong parseParsableLong(
      @Nullable ParsableLong num,
      Function<ParamDefinition, Parameter> paramParser,
      long maxLimit,
      String path) {
    if (num == null) {
      return null;
    }

    long retries;
    if (num.isLong()) {
      retries = num.getLong();
    } else {
      ParamDefinition paramDef = ParamDefinition.buildParamDefinition(PARAM_NAME, num.asString());
      String retriesParam = paramParser.apply(paramDef).asString();
      try {
        retries = Long.parseLong(retriesParam);
      } catch (NumberFormatException ne) {
        throw new IllegalArgumentException(
            String.format(
                "%s has an invalid value [%s] with an error: %s",
                path, retriesParam, ne.getMessage()));
      }
    }
    Checks.checkTrue(
        retries >= 0 && retries <= maxLimit,
        "%s value [%s] cannot be negative or more than system limit: %s",
        path,
        retries,
        maxLimit);
    return ParsableLong.of(retries);
  }

  private static ParsableLong getParsedErrorRetryLimit(
      @Nullable ParsableLong number,
      Function<ParamDefinition, Parameter> paramParser,
      String path) {
    return parseParsableLong(number, paramParser, Constants.MAX_ERROR_RETRY_LIMIT_SECS, path);
  }

  private static ParsableLong getParsedPlatformRetryLimit(
      @Nullable ParsableLong number,
      Function<ParamDefinition, Parameter> paramParser,
      String path) {
    return parseParsableLong(number, paramParser, Constants.MAX_PLATFORM_RETRY_LIMIT_SECS, path);
  }

  private static ParsableLong getParsedTimeoutRetryLimit(
      @Nullable ParsableLong number,
      Function<ParamDefinition, Parameter> paramParser,
      String path) {
    return parseParsableLong(number, paramParser, Constants.MAX_TIMEOUT_RETRY_LIMIT_SECS, path);
  }

  private static ParsableLong getParsedRetryExponent(
      @Nullable ParsableLong number,
      Function<ParamDefinition, Parameter> paramParser,
      String path) {
    return parseParsableLong(number, paramParser, Integer.MAX_VALUE, path);
  }

  private static RetryPolicy.Backoff getParsedBackoff(
      @Nullable RetryPolicy.Backoff backoff, Function<ParamDefinition, Parameter> paramParser) {
    if (backoff == null) {
      return null;
    }

    return switch (backoff.getType()) {
      case FIXED_BACKOFF -> {
        RetryPolicy.FixedBackoff fixedBackoff = (RetryPolicy.FixedBackoff) backoff;
        yield RetryPolicy.FixedBackoff.builder()
            .errorRetryBackoffInSecs(
                getParsedErrorRetryLimit(
                    fixedBackoff.getErrorRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_ERROR_RETRY_BACKOFF_IN_SECS))
            .platformRetryBackoffInSecs(
                getParsedPlatformRetryLimit(
                    fixedBackoff.getPlatformRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_PLATFORM_RETRY_BACKOFF_IN_SECS))
            .timeoutRetryBackoffInSecs(
                getParsedTimeoutRetryLimit(
                    fixedBackoff.getTimeoutRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_TIMEOUT_RETRY_BACKOFF_IN_SECS))
            .build();
      }
      case EXPONENTIAL_BACKOFF -> {
        RetryPolicy.ExponentialBackoff exponentialBackoff =
            (RetryPolicy.ExponentialBackoff) backoff;
        yield RetryPolicy.ExponentialBackoff.builder()
            .errorRetryBackoffInSecs(
                getParsedErrorRetryLimit(
                    exponentialBackoff.getErrorRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_ERROR_RETRY_BACKOFF_IN_SECS))
            .errorRetryExponent(
                getParsedRetryExponent(
                    exponentialBackoff.getErrorRetryExponent(),
                    paramParser,
                    BACKOFF_ERROR_RETRY_EXPONENT))
            .errorRetryLimitInSecs(
                getParsedErrorRetryLimit(
                    exponentialBackoff.getErrorRetryLimitInSecs(),
                    paramParser,
                    BACKOFF_ERROR_RETRY_LIMIT_IN_SECS))
            .platformRetryBackoffInSecs(
                getParsedPlatformRetryLimit(
                    exponentialBackoff.getPlatformRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_PLATFORM_RETRY_BACKOFF_IN_SECS))
            .platformRetryExponent(
                getParsedRetryExponent(
                    exponentialBackoff.getPlatformRetryExponent(),
                    paramParser,
                    BACKOFF_PLATFORM_RETRY_EXPONENT))
            .platformRetryLimitInSecs(
                getParsedPlatformRetryLimit(
                    exponentialBackoff.getPlatformRetryLimitInSecs(),
                    paramParser,
                    BACKOFF_PLATFORM_RETRY_LIMIT_IN_SECS))
            .timeoutRetryBackoffInSecs(
                getParsedTimeoutRetryLimit(
                    exponentialBackoff.getTimeoutRetryBackoffInSecs(),
                    paramParser,
                    BACKOFF_TIMEOUT_RETRY_BACKOFF_IN_SECS))
            .timeoutRetryExponent(
                getParsedRetryExponent(
                    exponentialBackoff.getTimeoutRetryExponent(),
                    paramParser,
                    BACKOFF_TIMEOUT_RETRY_EXPONENT))
            .timeoutRetryLimitInSecs(
                getParsedTimeoutRetryLimit(
                    exponentialBackoff.getTimeoutRetryLimitInSecs(),
                    paramParser,
                    BACKOFF_TIMEOUT_RETRY_LIMIT_IN_SECS))
            .build();
      }
    };
  }
}
