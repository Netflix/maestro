package com.netflix.maestro.signal.metrics;

/**
 * Constants for signal metrics.
 *
 * @author jun-he
 */
public final class MetricConstants extends com.netflix.maestro.engine.metrics.MetricConstants {
  private MetricConstants() {}

  /** Metric to count for the signal trigger matched subscription found. */
  public static final String SIGNAL_TRIGGER_MATCH_FOUND = "signaltrigger.match.found";

  /** Metric to count for the signal trigger subscription created. */
  public static final String SIGNAL_TRIGGER_SUBSCRIPTION_CREATED =
      "timetrigger.subscription.created";

  /** Successes in step trigger match message processing. */
  public static final String SIGNAL_TRIGGER_MATCH_SUCCESS = "signaltrigger.match.success";

  /** Failures in step trigger match message processing. */
  public static final String SIGNAL_TRIGGER_MATCH_FAILURE = "signaltrigger.match.failure";

  /** Successes in the signal trigger execution message processing. */
  public static final String SIGNAL_TRIGGER_EXECUTION_SUCCESS = "signaltrigger.execution.success";

  /** Failures in the signal trigger execution message processing. */
  public static final String SIGNAL_TRIGGER_EXECUTION_FAILURE = "signaltrigger.execution.failure";
}
