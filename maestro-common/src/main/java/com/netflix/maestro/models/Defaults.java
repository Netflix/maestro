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
package com.netflix.maestro.models;

import com.netflix.maestro.models.definition.FailureMode;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.ParamMode;
import java.time.ZoneId;
import java.util.TimeZone;

/** Class to hold the user facing default values for unset fields. */
public final class Defaults {

  private Defaults() {}

  /** Default timeout limit in seconds. */
  public static final long DEFAULT_TIME_OUT_LIMIT_IN_MILLIS = 10 * 24 * 3600 * 1000L; // 10 days

  /** By default, a new workflow version is active. */
  public static final boolean DEFAULT_WORKFLOW_ACTIVE_FLAG = true;

  /** Defaults for fixed retry delay for platform errors. */
  private static final long DEFAULT_FIXED_PLATFORM_RETRY_BACKOFF_SECS = 60L;

  /** Defaults for fixed retry delay for user errors. */
  private static final long DEFAULT_FIXED_USER_RETRY_BACKOFF_SECS = 60L;

  /** Defaults for exponential retry exponent for user errors. */
  private static final int DEFAULT_ERROR_RETRY_EXPONENT = 2;

  /** Defaults for exponential retry base backoff for user errors. */
  private static final long DEFAULT_BASE_ERROR_RETRY_BACKOFF_SECS = 60L;

  /** Defaults for exponential max retry limit for user errors. */
  private static final long DEFAULT_ERROR_RETRY_LIMIT_SECS = 600L;

  /** Defaults for exponential retry exponent for platform errors. */
  private static final int DEFAULT_PLATFORM_RETRY_EXPONENT = 2;

  /** Defaults for exponential retry base backoff for platform errors. */
  private static final long DEFAULT_BASE_PLATFORM_RETRY_BACKOFF_SECS = 60L;

  /** Defaults for exponential max retry limit for platform errors. */
  private static final long DEFAULT_PLATFORM_RETRY_LIMIT_SECS = 3600L;

  /** Default Exponential backoff. */
  public static final RetryPolicy.ExponentialBackoff DEFAULT_EXPONENTIAL_BACK_OFF =
      RetryPolicy.ExponentialBackoff.builder()
          .errorRetryExponent(DEFAULT_ERROR_RETRY_EXPONENT)
          .errorRetryBackoffInSecs(DEFAULT_BASE_ERROR_RETRY_BACKOFF_SECS)
          .errorRetryLimitInSecs(DEFAULT_ERROR_RETRY_LIMIT_SECS)
          .platformRetryBackoffInSecs(DEFAULT_BASE_PLATFORM_RETRY_BACKOFF_SECS)
          .platformRetryExponent(DEFAULT_PLATFORM_RETRY_EXPONENT)
          .platformRetryLimitInSecs(DEFAULT_PLATFORM_RETRY_LIMIT_SECS)
          .build();

  /** Default Fixed backoff. */
  public static final RetryPolicy.FixedBackoff DEFAULT_FIXED_BACK_OFF =
      RetryPolicy.FixedBackoff.builder()
          .platformRetryBackoffInSecs(DEFAULT_FIXED_PLATFORM_RETRY_BACKOFF_SECS)
          .errorRetryBackoffInSecs(DEFAULT_FIXED_USER_RETRY_BACKOFF_SECS)
          .build();

  private static final long DEFAULT_USER_RETRY_LIMIT = 2L;
  private static final long DEFAULT_PLATFORM_RETRY_LIMIT = 10L;

  /** Default retry policy if unset. */
  public static final RetryPolicy DEFAULT_RETRY_POLICY =
      RetryPolicy.builder()
          .errorRetryLimit(DEFAULT_USER_RETRY_LIMIT)
          .platformRetryLimit(DEFAULT_PLATFORM_RETRY_LIMIT)
          .backoff(DEFAULT_EXPONENTIAL_BACK_OFF)
          .build();

  /** Default step failure mode if unset. */
  public static final FailureMode DEFAULT_FAILURE_MODE = FailureMode.FAIL_AFTER_RUNNING;

  /** Constant for DEFAULT_RUN_STRATEGY. */
  public static final RunStrategy DEFAULT_RUN_STRATEGY = RunStrategy.create("SEQUENTIAL");

  /** Default parallelism in maestro for parallel run strategy. */
  public static final long DEFAULT_PARALLELISM = 20L;

  /** Default tag list if unset. */
  public static final TagList DEFAULT_TAG_LIST = TagList.EMPTY_TAG_LIST;

  /** Default step instance initial status. */
  public static final StepInstance.Status DEFAULT_STEP_INSTANCE_INITIAL_STATUS =
      StepInstance.Status.NOT_CREATED;

  /** Default Time Zone. * */
  public static final TimeZone DEFAULT_TIMEZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));

  /** Default Param Mode. */
  public static final ParamMode DEFAULT_PARAM_MODE = ParamMode.MUTABLE;

  /** Default flag to keep subworkflow execution synchronously. */
  public static final Boolean DEFAULT_SUBWORKFLOW_SYNC_FLAG = Boolean.TRUE;

  /** Default flag to pass down all workflow params from parents to the subworkflow. */
  public static final Boolean DEFAULT_SUBWORKFLOW_EXPLICIT_FLAG = Boolean.FALSE;

  /** Default concurrency limit for all step for a given workflow id. */
  public static final long DEFAULT_STEP_CONCURRENCY = 20;

  /**
   * Default concurrency limit for instance and step for better concurrency control. It is used when
   * the instance_step_concurrency is unset and disabled. Then Maestro will make sure to only launch
   * default number of workflow instances in each level of the DAG tree. But it will not check the
   * number of leaf steps so to protect Maestro but not slow down users. Set the value a bit higher
   * than DEFAULT_FOREACH_CONCURRENCY.
   */
  public static final long DEFAULT_INSTANCE_STEP_CONCURRENCY = 100;

  /** Default foreach step strict ordering flag. */
  public static final Boolean DEFAULT_FOREACH_STRICT_ORDERING_ENABLED = Boolean.FALSE;

  /** Default run policy for restart. */
  public static final RunPolicy DEFAULT_RESTART_POLICY = RunPolicy.RESTART_FROM_INCOMPLETE;
}
