package com.netflix.maestro.engine.metrics;

/** Class for SQS related Metric constants. */
public class AwsMetricConstants {
  protected AwsMetricConstants() {}

  /** SQS publish success metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC =
      "sqs.timetrigger.publish.success";

  /** SQS publish failures metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_FAILURE_METRIC =
      "sqs.timetrigger.publish.failure";

  /** SQS publish success metric for time trigger. */
  public static final String SQS_SIGNAL_PUBLISH_SUCCESS_METRIC = "sqs.signal.publish.success";

  /** SQS publish failures metric for time trigger. */
  public static final String SQS_SIGNAL_PUBLISH_FAILURE_METRIC = "sqs.signal.publish.failure";
}
