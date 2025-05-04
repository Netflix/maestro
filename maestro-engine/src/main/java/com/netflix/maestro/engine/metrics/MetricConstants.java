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
package com.netflix.maestro.engine.metrics;

/** Class for Metric constants such as keys / tags for engine package. */
public final class MetricConstants extends com.netflix.maestro.metrics.MetricConstants {

  private MetricConstants() {}

  /** Candidate selected tag for launch. */
  public static final String CANDIDATE_TAG_VALUE = "candidate";

  /** Tag for terminate job due to clean up. */
  public static final String CLEANUP_TAG_VALUE = "cleanup";

  /** Tag for empty job returned error type. */
  public static final String EMPTY_JOB_TAG_VALUE = "emptyjob";

  /** Non-candidate selected tag for launch. */
  public static final String NON_CANDIDATE_TAG_VALUE = "noncandidate";

  /** Non transient tag for error type metric. */
  public static final String NONTRANSIENT_TAG_VALUE = "nontransient";

  /** Tag for terminate due to upstream terminate called. */
  public static final String TERMINATE_TAG_VALUE = "terminate";

  /** Tag for terminate CMB errors. */
  public static final String TERMINATE_ERROR_TAG_VALUE = "terminateerror";

  /** Transient tag for error type metric. */
  public static final String TRANSIENT_TAG_VALUE = "transient";

  /** Metric name for onStepTerminated events, further categorized by tags. */
  public static final String ON_STEP_TERMINATED = "on.step.terminated";

  /** Tag name for onStepTerminated events. */
  public static final String STEP_STATE_TAG = "step.state";

  /** Tag name for onStepTerminated events. */
  public static final String STEP_STATUS_TAG = "step.status";

  /** Workflow instance start delay in milliseconds: CREATED -> IN_PROGRESS. */
  public static final String WORKFLOW_START_DELAY_METRIC = "workflow.instance.start.delay.ms";

  /** Step init delay in milliseconds: CREATED -> INITIALIZED. */
  public static final String STEP_INITIALIZE_DELAY_METRIC = "step.initialize.delay.ms";

  /** Metric name for step launch action. */
  public static final String STEP_LAUNCHED_METRIC = "step.launch";

  /** Status tag, used by various metrics. */
  public static final String STATUS_TAG = "status";

  /** Success value for status tag {@link #STATUS_TAG}. */
  public static final String STATUS_TAG_VALUE_SUCCESS = "success";

  /** Failure value for status tag {@link #STATUS_TAG}. */
  public static final String STATUS_TAG_VALUE_FAILURE = "failure";

  /** Workflow done status metric. */
  public static final String WORKFLOW_DONE_METRIC = "on.workflow.done";

  /** Status tag, used by various metrics. */
  public static final String INITIATOR_DEPTH_TAG = "depth";

  /** Metrics for MaestroFinalFlowStatusCallback. */
  public static final String FINAL_FLOW_STATUS_CALL_BACK_METRIC = "final.flow.status.callback";

  /** Metrics for step runtime manager terminate exceptions . */
  public static final String STEP_RUNTIME_MANAGER_TERMINATE_EXCEPTION =
      "stepruntime.manager.terminate.exception";

  /** Metrics runstrategy stopped instances. */
  public static final String RUNSTRATEGY_STOPPED_INSTANCES_METRIC = "runstrategy.stopped.instances";

  /** Metrics jobevents publish failures. */
  public static final String JOB_EVENT_PUBLISH_FAILURE_METRIC = "jobevent.publish.failure";
}
