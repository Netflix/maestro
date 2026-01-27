package com.netflix.maestro.signal.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.signal.SignalTriggerDef;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for SignalTriggerSubscriptionClient class.
 *
 * @author jun-he
 */
public class SignalTriggerSubscriptionClientTest extends MaestroBaseTest {
  private MaestroSignalBrokerDao brokerDao;
  private MaestroMetricRepo metricRepo;
  private SignalTriggerSubscriptionClient client;

  @Before
  public void setup() {
    brokerDao = mock(MaestroSignalBrokerDao.class);
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    client = new SignalTriggerSubscriptionClient(brokerDao, metricRepo);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpsertTriggerSubscription() throws Exception {
    SignalTrigger def =
        loadObject("fixtures/signal_triggers/signal_trigger_simple.json", SignalTrigger.class);
    Workflow workflow = Workflow.builder().id("test-wf").signalTriggers(List.of(def)).build();
    TriggerUuids currUuids =
        TriggerUuids.builder().signalTriggerUuids(Map.of("uuid1", 1, "uuid0", 0)).build();
    TriggerUuids prevUuids = TriggerUuids.builder().signalTriggerUuids(Map.of("uuid1", 1)).build();
    client.upsertTriggerSubscription(null, workflow, currUuids, prevUuids);
    ArgumentCaptor<List> defs = ArgumentCaptor.forClass(List.class);
    verify(brokerDao, times(1)).subscribeWorkflowTriggers(any(), defs.capture());
    var captured = defs.getValue();
    assertEquals(1, captured.size());
    assertEquals(new SignalTriggerDef("test-wf", "uuid0", def), captured.getFirst());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_SUBSCRIPTION_CREATED,
                SignalTriggerSubscriptionClient.class)
            .count());
  }

  @Test
  public void testUpsertTriggerSubscriptionWithoutUpdate() throws Exception {
    SignalTrigger def =
        loadObject("fixtures/signal_triggers/signal_trigger_simple.json", SignalTrigger.class);
    Workflow workflow = Workflow.builder().id("test-wf").signalTriggers(List.of(def)).build();
    client.upsertTriggerSubscription(null, workflow, null, null);
    verify(brokerDao, times(0)).subscribeWorkflowTriggers(any(), any());

    TriggerUuids currUuids = TriggerUuids.builder().signalTriggerUuids(Map.of("uuid0", 0)).build();
    TriggerUuids prevUuids = TriggerUuids.builder().signalTriggerUuids(Map.of("uuid0", 0)).build();
    client.upsertTriggerSubscription(null, workflow, currUuids, prevUuids);
    verify(brokerDao, times(0)).subscribeWorkflowTriggers(any(), any());
    assertEquals(
        0L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_SUBSCRIPTION_CREATED,
                SignalTriggerSubscriptionClient.class)
            .count());
  }
}
