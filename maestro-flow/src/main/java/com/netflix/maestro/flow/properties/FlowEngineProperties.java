package com.netflix.maestro.flow.properties;

import com.netflix.maestro.database.DatabaseConfiguration;
import java.util.concurrent.TimeUnit;

/**
 * Maestro internal flow engine properties.
 *
 * @author jun-he
 */
@SuppressWarnings("checkstyle:MagicNumber")
public class FlowEngineProperties implements DatabaseConfiguration {
  private static final String MAX_GROUP_NUM_PROPERTY_NAME = "max.group.num";
  private static final long MAX_GROUP_NUM_DEFAULT_VALUE = 3;

  private static final String ACTOR_RETRY_INTERVAL_PROPERTY_NAME = "actor.retry.interval.millis";
  private static final long ACTOR_RETRY_INTERVAL_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(5);

  private static final String GROUP_FLOW_FETCH_LIMIT_PROPERTY_NAME = "group.flow.fetch.limit";
  private static final long GROUP_FLOW_FETCH_LIMIT_DEFAULT_VALUE = 100;

  private static final String FLOW_REFRESH_INTERVAL_PROPERTY_NAME = "flow.refresh.interval.millis";
  private static final long FLOW_REFRESH_INTERVAL_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(3);

  private static final String FLOW_RECONCILIATION_INTERVAL_PROPERTY_NAME =
      "flow.reconciliation.interval.millis";
  private static final long FLOW_RECONCILIATION_INTERVAL_DEFAULT_VALUE =
      TimeUnit.SECONDS.toMillis(30);

  private static final String GROUP_HEARTBEAT_INTERVAL_PROPERTY_NAME =
      "group.heartbeat.interval.millis";
  private static final long GROUP_HEARTBEAT_INTERVAL_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(12);

  private static final String EXPIRATION_PROPERTY_NAME = "expiration.secs";
  private static final long EXPIRATION_DEFAULT_VALUE = 60;

  private static final String INITIAL_MAINTENANCE_DELAY_PROPERTY_NAME =
      "initial.maintenance.delay.millis";
  private static final long INITIAL_MAINTENANCE_DELAY_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(5);

  private static final String MAINTENANCE_DELAY_PROPERTY_NAME = "maintenance.delay.millis";
  private static final long MAINTENANCE_DELAY_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(3);

  public long getMaxGroupNum() {
    return getLongProperty(MAX_GROUP_NUM_PROPERTY_NAME, MAX_GROUP_NUM_DEFAULT_VALUE);
  }

  public long getActorErrorRetryIntervalInMillis() {
    return getLongProperty(ACTOR_RETRY_INTERVAL_PROPERTY_NAME, ACTOR_RETRY_INTERVAL_DEFAULT_VALUE);
  }

  public long getGroupFlowFetchLimit() {
    return getLongProperty(
        GROUP_FLOW_FETCH_LIMIT_PROPERTY_NAME, GROUP_FLOW_FETCH_LIMIT_DEFAULT_VALUE);
  }

  public long getFlowRefreshIntervalInMillis() {
    return getLongProperty(
        FLOW_REFRESH_INTERVAL_PROPERTY_NAME, FLOW_REFRESH_INTERVAL_DEFAULT_VALUE);
  }

  public long getFlowReconciliationIntervalInMillis() {
    return getLongProperty(
        FLOW_RECONCILIATION_INTERVAL_PROPERTY_NAME, FLOW_RECONCILIATION_INTERVAL_DEFAULT_VALUE);
  }

  public long getHeartbeatIntervalInMillis() {
    return getLongProperty(
        GROUP_HEARTBEAT_INTERVAL_PROPERTY_NAME, GROUP_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
  }

  public long getExpirationInSecs() {
    return getLongProperty(EXPIRATION_PROPERTY_NAME, EXPIRATION_DEFAULT_VALUE);
  }

  public long getInitialMaintenanceDelayInMillis() {
    return getLongProperty(
        INITIAL_MAINTENANCE_DELAY_PROPERTY_NAME, INITIAL_MAINTENANCE_DELAY_DEFAULT_VALUE);
  }

  public long getMaintenanceDelayInMillis() {
    return getLongProperty(MAINTENANCE_DELAY_PROPERTY_NAME, MAINTENANCE_DELAY_DEFAULT_VALUE);
  }

  // todo placeholder
  public String getEngineAddress() {
    return "localhost:8080";
  }
}
