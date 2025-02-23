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

import com.netflix.maestro.models.definition.StepType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Class to hold the constants. */
public final class Constants {
  private Constants() {}

  /** qualifier for maestro related constants. */
  public static final String MAESTRO_QUALIFIER = "maestro";

  /** Reserved maestro prefix for internal usage. */
  public static final String MAESTRO_PREFIX = "maestro_";

  /** Reserved maestro suffix for internal usage. */
  public static final String MAESTRO_SUFFIX = "_maestro";

  /** Maestro flow engine proxy task name. */
  public static final String MAESTRO_TASK_NAME = "MAESTRO_TASK";

  /** Maestro flow engine start task name. */
  public static final String DEFAULT_START_TASK_NAME = MAESTRO_PREFIX + "start";

  /** Maestro flow engine end task name. */
  public static final String DEFAULT_END_TASK_NAME = MAESTRO_PREFIX + "end";

  /** Maestro internal proxy flow task name set for user defined step. */
  public static final Set<String> USER_DEFINED_TASKS =
      Collections.singleton(Constants.MAESTRO_TASK_NAME);

  /** Maestro workflow instance summary field name. */
  public static final String WORKFLOW_SUMMARY_FIELD = MAESTRO_PREFIX + "workflow_summary";

  /** Maestro workflow instance runtime summary field name. */
  public static final String WORKFLOW_RUNTIME_SUMMARY_FIELD =
      MAESTRO_PREFIX + "workflow_runtime_summary";

  /** Maestro step instance runtime summary field name. */
  public static final String STEP_RUNTIME_SUMMARY_FIELD = MAESTRO_PREFIX + "step_runtime_summary";

  /** All maestro step dependencies field name. */
  public static final String ALL_STEP_DEPENDENCIES_FIELD = MAESTRO_PREFIX + "all_step_dependencies";

  /** Maestro foreach inline workflow prefix. */
  public static final String FOREACH_INLINE_WORKFLOW_PREFIX =
      MAESTRO_PREFIX + StepType.FOREACH.getType();

  /** Maestro foreach step loop param name. */
  public static final String LOOP_PARAMS_NAME = "loop_params";

  /** Maestro foreach step loop index param name. */
  public static final String INDEX_PARAM_NAME = "loop_index";

  /** System-wide foreach loop iteration limit. */
  public static final int FOREACH_ITERATION_LIMIT = 25 * 1000;

  /** Maximum limit for foreach concurrency. */
  public static final int FOREACH_CONCURRENCY_MAX_LIMIT = 500;

  /** Maximum limit for step concurrency. */
  public static final long STEP_CONCURRENCY_MAX_LIMIT = 1000L;

  /** Maximum limit for workflow concurrency. */
  public static final long WORKFLOW_CONCURRENCY_MAX_LIMIT = 1000L;

  /** Maximum limit for instance step concurrency. */
  public static final long INSTANCE_STEP_CONCURRENCY_MAX_LIMIT = 1000L;

  /** Maestro step condition parameter name for every Maestro step. */
  public static final String STEP_SATISFIED_FIELD = MAESTRO_PREFIX + "step_satisfied";

  /** Maestro ID length limit. */
  public static final int ID_LENGTH_LIMIT = 128;

  /** Maestro name length limit. */
  public static final int NAME_LENGTH_LIMIT = 256;

  /** Maestro field length limit. */
  public static final int FIELD_SIZE_LIMIT = 16384;

  /**
   * The maximum number of workflow instances to get started. It is used as both DB batch size and
   * job event trunk size in RunWorkflowInstancesJobEvent for foreach case.
   */
  public static final int START_BATCH_LIMIT = 10;

  /**
   * The maximum number of workflow instances to get dequeued to run. It is used when run strategy
   * dao dequeues instances when receiving a StartWorkflowJobEvent. If more dequeue instances exist,
   * will send another StartWorkflowJobEvent to dequeue instances again.
   */
  public static final int DEQUEUE_SIZE_LIMIT = 50;

  /**
   * The maximum number of workflow instances to get terminated in a batch. It is used when
   * terminating workflows by workflow id as both DB batch size and job event batch size in
   * WorkflowInstanceUpdateJobEvent and TerminateWorkflowInstancesJobEvent.
   */
  public static final int TERMINATE_BATCH_LIMIT = 10;

  /** The maximum number of workflow instances to get unblocked in a batch. */
  public static final int UNBLOCK_BATCH_SIZE = 100;

  /** The maximum number of rows of workflow data to get deleted in a batch. */
  public static final int BATCH_DELETION_LIMIT = 200;

  /**
   * Invisible time for resending job event, e.g. resending start job event if queued instances are
   * larger than the limit. If needed, make it configurable.
   */
  public static final long RESEND_JOB_EVENT_DELAY_IN_MILLISECONDS = TimeUnit.SECONDS.toMillis(2);

  /**
   * The maximum number of steps defined in workflow definition. Note that this limit can only be
   * bumped up and cannot be decreased as it is related to the pushed data.
   * <li>Internal flow engine might not handle too large workflow well.
   * <li>UI might be slow to render large workflows with too many nodes.
   * <li>Enforce the best practice to avoid a workflow with too many steps hard coded in a single
   *     workflow definition.
   */
  public static final int STEP_LIST_SIZE_LIMIT = 300;

  /** Workflow depth limit for a nested workflow with subworkflow and foreach steps. */
  public static final int WORKFLOW_DEPTH_LIMIT = 10;

  /** maximum system retry limit for user errors or platform errors. */
  public static final int MAX_RETRY_LIMIT = 99;

  /** maximum retry wait limit in secs for user errors. */
  public static final int MAX_ERROR_RETRY_LIMIT_SECS = 24 * 3600; // 1 day

  /** maximum retry wait limit for platform errors. */
  public static final int MAX_PLATFORM_RETRY_LIMIT_SECS = 24 * 3600; // 1 day

  /** maximum retry wait limit for timeout errors. */
  public static final int MAX_TIMEOUT_RETRY_LIMIT_SECS = 24 * 3600; // 1 day

  /** Max timeout limit in milliseconds. */
  public static final long MAX_TIME_OUT_LIMIT_IN_MILLIS = TimeUnit.DAYS.toMillis(120); // 120 days

  /**
   * batch size for when we query step instances for foreach and subworkflow steps for rollup
   * calculation.
   */
  public static final int BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY = 50;

  /** the latest workflow instance run. */
  public static final String LATEST_INSTANCE_RUN = "LATEST";

  /** the constant indicating the latest workflow instance run id or step instance attempt id. */
  public static final long LATEST_ONE = 0L;

  /** Workflow is inactive if active_version_id is equals to 0. */
  public static final long INACTIVE_VERSION_ID = 0L;

  /** Workflow rollup overview reference delimiter. */
  public static final String REFERENCE_DELIMITER = ":";

  /** Maestro user defined param map size limit. */
  public static final int PARAM_MAP_SIZE_LIMIT = 128;

  /** Enum to categorize workflow version. */
  public enum WorkflowVersion {
    /** active workflow version. */
    ACTIVE,
    /** latest workflow version. */
    LATEST,
    /** default workflow version, either the active one or the latest one if no active. */
    DEFAULT,
    /** exact version id. */
    EXACT;

    /** Static constructor. */
    public static WorkflowVersion of(String version) {
      String vid = version.toUpperCase(Locale.US);
      if (ACTIVE.name().equals(vid)) {
        return ACTIVE;
      } else if (LATEST.name().equals(vid)) {
        return LATEST;
      } else if (DEFAULT.name().equals(vid)) {
        return DEFAULT;
      } else {
        return EXACT;
      }
    }
  }

  /** Metadata key for parameter source. */
  public static final String METADATA_SOURCE_KEY = "source";

  /** Metadata key for internal parameter mode. */
  public static final String METADATA_INTERNAL_PARAM_MODE = "internal_mode";

  /** Step ID Param Key. */
  public static final String STEP_ID_PARAM = "step_id";

  /** Step Instance ID Param Key. */
  public static final String STEP_INSTANCE_ID_PARAM = "step_instance_id";

  /** Step Instance ID Param Key. */
  public static final String STEP_ATTEMPT_ID_PARAM = "step_attempt_id";

  /** Step Instance UUID Param Key. */
  public static final String STEP_INSTANCE_UUID_PARAM = "step_instance_uuid";

  /** Workflow Instance ID Param Key. */
  public static final String WORKFLOW_INSTANCE_ID_PARAM = "workflow_instance_id";

  /** Step Type Info Param Key. */
  public static final String STEP_TYPE_INFO_PARAM = "step_type_info";

  /** Workflow ID Param Key. */
  public static final String WORKFLOW_ID_PARAM = "workflow_id";

  /** Workflow UUID Param Key. */
  public static final String WORKFLOW_UUID_PARAM = "workflow_uuid";

  /** Workflow Run ID Param Key. */
  public static final String WORKFLOW_RUN_ID_PARAM = "workflow_run_id";

  /** Correlation ID Param Key. */
  public static final String CORRELATION_ID_PARAM = "correlation_id";

  /** Workflow owner param key. */
  public static final String WORKFLOW_OWNER_PARAM = "owner";

  /** Param for time trigger timezone, first timezone in cron time triggers. */
  public static final String FIRST_TIME_TRIGGER_TIMEZONE_PARAM = "FIRST_TIME_TRIGGER_TIMEZONE";

  /**
   * Param for initiator timezone, in case where it is time triggered, to be sent by cron time
   * trigger.
   */
  public static final String INITIATOR_TIMEZONE_PARAM = "INITIATOR_TIMEZONE";

  /** Param name for initiator type. */
  public static final String INITIATOR_TYPE_PARAM = "INITIATOR_TYPE";

  /** Param value for dry run initiator type. */
  public static final String DRY_RUN_INITIATOR_TYPE_VALUE = "VALIDATION";

  /** Param name for the username of a manual initiator. */
  public static final String INITIATOR_RUNNER_NAME = "INITIATOR_RUNNER_NAME";

  /** Workflow instance run request run policy param key. */
  public static final String WORKFLOW_RUN_POLICY_PARAM = "RUN_POLICY";

  /** Step instance status param name key used in SEL to retrieve the step status. */
  public static final String STEP_STATUS_PARAM = "MAESTRO_STEP_STATUS";

  /** Step instance end time param name key used in SEL to retrieve the step end time. */
  public static final String STEP_END_TIME_PARAM = "MAESTRO_STEP_END_TIME";

  /**
   * Match all instances. Special value to denote a breakpoint which is set to match all wf
   * instances.
   */
  public static final long MATCH_ALL_WORKFLOW_INSTANCES = 0L;

  /**
   * Match all versions. Special value to denote a breakpoint which is set to match all wf versions.
   */
  public static final long MATCH_ALL_WORKFLOW_VERSIONS = 0L;

  /** Match all runs. Special value to denote a breakpoint which is set to match all wf runs. */
  public static final long MATCH_ALL_RUNS = 0L;

  /**
   * Match all attempts. Special value to denote a breakpoint which is set to match all step
   * attempts.
   */
  public static final long MATCH_ALL_STEP_ATTEMPTS = 0L;

  /** The max number of timeline events queried from the DB. */
  public static final int TIMELINE_EVENT_SIZE_LIMIT = 300;

  /** Additional param names reserved for the internal usage. */
  public static final List<String> RESERVED_PARAM_NAMES =
      Collections.unmodifiableList(
          Arrays.asList(
              MAESTRO_QUALIFIER,
              "Maestro",
              "MAESTRO",
              "scheduler",
              "Scheduler",
              "SCHEDULER",
              "util",
              "Util",
              "UTIL",
              "params",
              "Params",
              "PARAMS",
              "AUTH_BLOB",
              "AUTH_DATA",
              "AUTH_ACTING_ON_BEHALF_OF_BLOB",
              "AUTH_TRIGGERING_ON_BEHALF_OF_BLOB",
              "AUTHORIZED_MANAGERS"));

  /** Minimum interval for any time trigger. */
  public static final long TIME_TRIGGER_MINIMUM_INTERVAL = TimeUnit.MINUTES.toMillis(1);

  /** Workflow create request data size limit used for validation. */
  public static final String WORKFLOW_CREATE_REQUEST_DATA_SIZE_LIMIT = "256KB";

  /** param's total size (in JSON format) limit for a workflow instance or a step instance. */
  public static final int JSONIFIED_PARAMS_STRING_SIZE_LIMIT = 750000;

  /** Defines limit for the query for step attempt state view. */
  public static final int STEP_ATTEMPT_STATE_LIMIT = 100;

  /**
   * System limit for the total number of leaf steps in a workflow instance DAG tree. The total
   * number of nodes in maestro DAG tree is also bounded by WORKFLOW_DEPTH_LIMIT multiplying by
   * TOTAL_LEAF_STEP_COUNT_LIMIT.
   */
  public static final int TOTAL_LEAF_STEP_COUNT_LIMIT = 1000 * 1000;

  /** reference to the largest character in use for generating the inline workflow id. */
  public static final String INLINE_WORKFLOW_ID_LARGEST_CHAR_IN_USE = "~";

  /** the number of components existed in an inline workflow id split by _. */
  public static final int INLINE_WORKFLOW_ID_SPLIT_COMPONENT_COUNT = 5;
}
