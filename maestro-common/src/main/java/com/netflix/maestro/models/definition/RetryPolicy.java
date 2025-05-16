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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.Defaults;
import java.util.Locale;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Step retry policy. */
@Builder(toBuilder = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"error_retry_limit", "platform_retry_limit", "timeout_retry_limit", "backoff"},
    alphabetic = true)
@JsonDeserialize(builder = RetryPolicy.RetryPolicyBuilder.class)
@Getter
@EqualsAndHashCode
public class RetryPolicy {
  /** Retry limit for user errors. */
  private final ParsableLong errorRetryLimit;

  /** Retry limit for platform errors. */
  private final ParsableLong platformRetryLimit;

  /** Retry limit for timeout errors. */
  private final ParsableLong timeoutRetryLimit;

  /** Backoff strategy. */
  private final Backoff backoff;

  /** Enums of supported BackOff policies. */
  public enum BackoffPolicyType {
    /** exponential backoff. */
    EXPONENTIAL_BACKOFF,
    /** fixed backoff. */
    FIXED_BACKOFF;

    /** Static creator. */
    @JsonCreator
    public static BackoffPolicyType create(String type) {
      return BackoffPolicyType.valueOf(type.toUpperCase(Locale.US));
    }
  }

  /**
   * Merge a given step retry policy with DEFAULT RETRY POLICY.
   *
   * @param policy retry policy
   * @return final retry policy
   */
  public static RetryPolicy tryMergeWithDefault(RetryPolicy policy) {
    RetryPolicy defaultRetryPolicy = Defaults.DEFAULT_RETRY_POLICY;
    RetryPolicy.RetryPolicyBuilder retryPolicyBuilder;
    if (policy != null) {
      retryPolicyBuilder = policy.toBuilder();
      // Merge from default.
      if (retryPolicyBuilder.errorRetryLimit == null) {
        retryPolicyBuilder.errorRetryLimit = defaultRetryPolicy.errorRetryLimit;
      }
      // Merge from default.
      if (retryPolicyBuilder.platformRetryLimit == null) {
        retryPolicyBuilder.platformRetryLimit = defaultRetryPolicy.platformRetryLimit;
      }
      // Merge from default.
      if (retryPolicyBuilder.timeoutRetryLimit == null) {
        retryPolicyBuilder.timeoutRetryLimit = defaultRetryPolicy.timeoutRetryLimit;
      }
      if (retryPolicyBuilder.backoff == null) {
        retryPolicyBuilder.backoff = defaultRetryPolicy.backoff;
      } else {
        retryPolicyBuilder.backoff = retryPolicyBuilder.backoff.mergeWithDefault();
      }
    } else {
      retryPolicyBuilder = defaultRetryPolicy.toBuilder();
    }
    return retryPolicyBuilder.build();
  }

  /** Retry Delay interface. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type",
      include = JsonTypeInfo.As.EXISTING_PROPERTY)
  @JsonSubTypes({
    @JsonSubTypes.Type(name = "EXPONENTIAL_BACKOFF", value = ExponentialBackoff.class),
    @JsonSubTypes.Type(name = "FIXED_BACKOFF", value = FixedBackoff.class)
  })
  public interface Backoff {
    /** BackoffPolicy type. */
    BackoffPolicyType getType();

    /** Get next retry delay for user errors. */
    int getNextRetryDelayForUserError(long errorRetries);

    /** Get next retry delay for platform errors. */
    int getNextRetryDelayForPlatformError(long platformRetries);

    /** Get next retry delay for timeout errors. */
    int getNextRetryDelayForTimeoutError(long timeoutRetries);

    /** Merge with default and get new backoff. */
    Backoff mergeWithDefault();
  }

  /** Exponential Backoff. */
  @Builder(toBuilder = true)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "error_retry_backoff_in_secs",
        "error_retry_exponent",
        "error_retry_limit_in_secs",
        "platform_retry_backoff_in_secs",
        "platform_retry_exponent",
        "platform_retry_limit_in_secs",
        "timeout_retry_backoff_in_secs",
        "timeout_retry_exponent",
        "timeout_retry_limit_in_secs"
      },
      alphabetic = true)
  @JsonDeserialize(builder = ExponentialBackoff.ExponentialBackoffBuilder.class)
  @Getter
  @EqualsAndHashCode
  public static class ExponentialBackoff implements Backoff {
    /** Base time in seconds to wait between retries for user errors. */
    private final ParsableLong errorRetryBackoffInSecs;

    /** Base exponent. */
    private final ParsableLong errorRetryExponent;

    /** Max time in seconds to wait between retries for user errors. */
    private final ParsableLong errorRetryLimitInSecs;

    /** Base time in seconds to wait between retries for platform errors. */
    private final ParsableLong platformRetryBackoffInSecs;

    /** Base exponent for platform errors. */
    private final ParsableLong platformRetryExponent;

    /** Max time in seconds to wait between retries for platform errors. */
    private final ParsableLong platformRetryLimitInSecs;

    /** Base time in seconds to wait between retries for timeout errors. */
    private final ParsableLong timeoutRetryBackoffInSecs;

    /** Base exponent for timeout errors. */
    private final ParsableLong timeoutRetryExponent;

    /** Max time in seconds to wait between retries for timeout errors. */
    private final ParsableLong timeoutRetryLimitInSecs;

    @Override
    public BackoffPolicyType getType() {
      return BackoffPolicyType.EXPONENTIAL_BACKOFF;
    }

    @Override
    public int getNextRetryDelayForUserError(long errorRetries) {
      long waitVal =
          (long)
              (errorRetryBackoffInSecs.getLong()
                  * Math.pow(errorRetryExponent.getLong(), errorRetries));
      return (int) Math.min(waitVal, errorRetryLimitInSecs.getLong());
    }

    @Override
    public int getNextRetryDelayForPlatformError(long platformRetries) {
      long waitVal =
          (long)
              (platformRetryBackoffInSecs.getLong()
                  * Math.pow(platformRetryExponent.getLong(), platformRetries));
      return (int) Math.min(waitVal, platformRetryLimitInSecs.getLong());
    }

    @Override
    public int getNextRetryDelayForTimeoutError(long timeoutRetries) {
      long waitVal =
          (long)
              (timeoutRetryBackoffInSecs.getLong()
                  * Math.pow(timeoutRetryExponent.getLong(), timeoutRetries));
      return (int) Math.min(waitVal, timeoutRetryLimitInSecs.getLong());
    }

    @Override
    public Backoff mergeWithDefault() {
      RetryPolicy.ExponentialBackoff defaultExponentialBackoff =
          Defaults.DEFAULT_EXPONENTIAL_BACK_OFF;
      RetryPolicy.ExponentialBackoff.ExponentialBackoffBuilder exponentialBackoffBuilder =
          this.toBuilder();
      if (exponentialBackoffBuilder.errorRetryBackoffInSecs == null) {
        exponentialBackoffBuilder.errorRetryBackoffInSecs =
            defaultExponentialBackoff.errorRetryBackoffInSecs;
      }
      if (exponentialBackoffBuilder.errorRetryLimitInSecs == null) {
        exponentialBackoffBuilder.errorRetryLimitInSecs =
            defaultExponentialBackoff.errorRetryLimitInSecs;
      }
      if (exponentialBackoffBuilder.errorRetryExponent == null) {
        exponentialBackoffBuilder.errorRetryExponent = defaultExponentialBackoff.errorRetryExponent;
      }
      if (exponentialBackoffBuilder.platformRetryBackoffInSecs == null) {
        exponentialBackoffBuilder.platformRetryBackoffInSecs =
            defaultExponentialBackoff.platformRetryBackoffInSecs;
      }
      if (exponentialBackoffBuilder.platformRetryLimitInSecs == null) {
        exponentialBackoffBuilder.platformRetryLimitInSecs =
            defaultExponentialBackoff.platformRetryLimitInSecs;
      }
      if (exponentialBackoffBuilder.platformRetryExponent == null) {
        exponentialBackoffBuilder.platformRetryExponent =
            defaultExponentialBackoff.platformRetryExponent;
      }
      if (exponentialBackoffBuilder.timeoutRetryBackoffInSecs == null) {
        exponentialBackoffBuilder.timeoutRetryBackoffInSecs =
            defaultExponentialBackoff.timeoutRetryBackoffInSecs;
      }
      if (exponentialBackoffBuilder.timeoutRetryLimitInSecs == null) {
        exponentialBackoffBuilder.timeoutRetryLimitInSecs =
            defaultExponentialBackoff.timeoutRetryLimitInSecs;
      }
      if (exponentialBackoffBuilder.timeoutRetryExponent == null) {
        exponentialBackoffBuilder.timeoutRetryExponent =
            defaultExponentialBackoff.timeoutRetryExponent;
      }
      return exponentialBackoffBuilder.build();
    }

    /** builder class for lombok and jackson. */
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class ExponentialBackoffBuilder {}
  }

  /** Fixed Backoff. */
  @Builder(toBuilder = true)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "error_retry_backoff_in_secs",
        "platform_retry_backoff_in_secs",
        "timeout_retry_backoff_in_secs"
      },
      alphabetic = true)
  @JsonDeserialize(builder = FixedBackoff.FixedBackoffBuilder.class)
  @Getter
  @EqualsAndHashCode
  public static class FixedBackoff implements Backoff {
    /** Constant wait between error retries. */
    private final ParsableLong errorRetryBackoffInSecs;

    /** Constant wait between platform retries. */
    private final ParsableLong platformRetryBackoffInSecs;

    /** Constant wait between timeout retries. */
    private final ParsableLong timeoutRetryBackoffInSecs;

    @Override
    public BackoffPolicyType getType() {
      return BackoffPolicyType.FIXED_BACKOFF;
    }

    @Override
    public int getNextRetryDelayForUserError(long errorRetries) {
      return errorRetryBackoffInSecs.asInt();
    }

    @Override
    public int getNextRetryDelayForPlatformError(long platformRetries) {
      return platformRetryBackoffInSecs.asInt();
    }

    @Override
    public int getNextRetryDelayForTimeoutError(long timeoutRetries) {
      return timeoutRetryBackoffInSecs.asInt();
    }

    @Override
    public Backoff mergeWithDefault() {
      RetryPolicy.FixedBackoff defaultFixedBackoff = Defaults.DEFAULT_FIXED_BACK_OFF;
      RetryPolicy.FixedBackoff.FixedBackoffBuilder fixedBackoffBuilder = this.toBuilder();
      if (fixedBackoffBuilder.errorRetryBackoffInSecs == null) {
        fixedBackoffBuilder.errorRetryBackoffInSecs = defaultFixedBackoff.errorRetryBackoffInSecs;
      }
      if (fixedBackoffBuilder.platformRetryBackoffInSecs == null) {
        fixedBackoffBuilder.platformRetryBackoffInSecs =
            defaultFixedBackoff.platformRetryBackoffInSecs;
      }
      if (fixedBackoffBuilder.timeoutRetryBackoffInSecs == null) {
        fixedBackoffBuilder.timeoutRetryBackoffInSecs =
            defaultFixedBackoff.timeoutRetryBackoffInSecs;
      }
      return fixedBackoffBuilder.build();
    }

    /** builder class for lombok and jackson. */
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class FixedBackoffBuilder {}
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class RetryPolicyBuilder {}
}
