package com.netflix.maestro.engine.metrics;

/** Class for SQS related Metric constants. */
public class AwsMetricConstants {
  protected AwsMetricConstants() {}

  /** SQS jobevents listener deserialization success metric. */
  public static final String SQS_JOB_EVENT_LISTENER_SUCCESS_METRIC = "sqs.event.listener.success";

  /** SQS jobevents listener deserialization failure metric. */
  public static final String SQS_JOB_EVENT_LISTENER_FAILURE_METRIC = "sqs.event.listener.failure";

  /**
   * SQS jobevents with high number of retries metric, which might indicate problematic messages.
   */
  public static final String SQS_EVENT_HIGH_NUMBER_OF_RETRIES = "sqs.event.high.retries";

  /** SQS Processor latency metric, in milliseconds. */
  public static final String SQS_PROCESSOR_LATENCY_METRIC = "sqs.processor.latency.ms";

  /** SQS job event type tag for metrics. */
  public static final String JOB_TYPE_TAG = "jobType";

  /** SQS jobevents publish success metric. */
  public static final String SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC = "sqs.event.publish.success";

  /** SQS jobevents publish failures metric. */
  public static final String SQS_JOB_EVENT_PUBLISH_FAILURE_METRIC = "sqs.event.publish.failure";

  /** SQS publish success metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC =
      "sqs.timetrigger.publish.success";

  /** SQS publish failures metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_FAILURE_METRIC =
      "sqs.timetrigger.publish.failure";
}
