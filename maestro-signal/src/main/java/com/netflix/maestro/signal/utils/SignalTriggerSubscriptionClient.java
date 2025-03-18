package com.netflix.maestro.signal.utils;

import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerDef;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Signal trigger subscription client.
 *
 * @author jun-he
 */
@Slf4j
@AllArgsConstructor
public class SignalTriggerSubscriptionClient implements TriggerSubscriptionClient {
  private final MaestroSignalBrokerDao brokerDao;
  private final MaestroMetrics metrics;

  @Override
  public void upsertTriggerSubscription(
      Connection conn, Workflow workflow, TriggerUuids current, TriggerUuids previous)
      throws SQLException {
    if (!ObjectHelper.isCollectionEmptyOrNull(workflow.getSignalTriggers())
        && current != null
        && current.getSignalTriggerUuids() != null) {
      var alreadySent =
          (previous != null && previous.getSignalTriggerUuids() != null)
              ? previous.getSignalTriggerUuids().keySet()
              : Collections.emptySet();

      // get uuids in current active version but not in the previous active version
      var signalTriggerDefs =
          current.getSignalTriggerUuids().entrySet().stream()
              .filter(entry -> !alreadySent.contains(entry.getKey()))
              .map(
                  entry ->
                      new SignalTriggerDef(
                          workflow.getId(),
                          entry.getKey(),
                          workflow.getSignalTriggers().get(entry.getValue())))
              .toList();
      if (!signalTriggerDefs.isEmpty()) {
        LOG.info(
            "Add signal trigger definitions [{}] for workflow [{}]",
            signalTriggerDefs,
            workflow.getId());
        brokerDao.subscribeWorkflowTriggers(conn, signalTriggerDefs);
        metrics.counter(MetricConstants.SIGNAL_TRIGGER_SUBSCRIPTION_CREATED, getClass());
      } else {
        LOG.info(
            "No signal triggers added for workflow [{}] as they have already been sent.",
            workflow.getId());
      }
    }
  }
}
