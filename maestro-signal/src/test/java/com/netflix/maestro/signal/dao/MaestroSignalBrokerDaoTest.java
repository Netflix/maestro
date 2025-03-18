package com.netflix.maestro.signal.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.api.SignalCreateRequest;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalOperator;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.signal.models.SignalInstanceRef;
import com.netflix.maestro.signal.models.SignalMatchDto;
import com.netflix.maestro.signal.models.SignalTriggerDef;
import com.netflix.maestro.signal.models.SignalTriggerDto;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import java.sql.Connection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for MaestroSignalBrokerDao class.
 *
 * @author jun-he
 */
public class MaestroSignalBrokerDaoTest extends MaestroBaseSignalDaoTest {
  private MaestroSignalBrokerDao brokerDao;
  private Connection conn;
  private MaestroSignalInstanceDao instanceDao;
  private MaestroSignalParamDao paramDao;
  private MaestroSignalTriggerDao triggerDao;
  private SignalQueueProducer queueProducer;

  @Before
  public void setup() throws Exception {
    instanceDao = Mockito.mock(MaestroSignalInstanceDao.class);
    paramDao = Mockito.mock(MaestroSignalParamDao.class);
    triggerDao = Mockito.mock(MaestroSignalTriggerDao.class);
    queueProducer = Mockito.mock(SignalQueueProducer.class);
    brokerDao =
        new MaestroSignalBrokerDao(
            DATA_SOURCE, MAPPER, CONFIG, METRICS, instanceDao, paramDao, triggerDao, queueProducer);
    conn = DATA_SOURCE.getConnection();
  }

  @After
  public void cleanup() throws Exception {
    conn.close();
  }

  @Test
  public void testAddSignal() throws Exception {
    SignalCreateRequest request =
        loadObject("fixtures/api/sample-signal-create-request.json", SignalCreateRequest.class);
    when(instanceDao.addSignalInstance(any(), any(), any(), any())).thenReturn(12L);
    var instance = brokerDao.addSignal(request);
    assertEquals("signal_a", instance.getName());
    assertEquals("test-request-id", instance.getInstanceId());
    assertEquals(12L, instance.getSeqId());
    assertNull(instance.getDetails());
    verify(instanceDao, times(1)).addSignalInstance(any(), any(), any(), any());
    verify(paramDao, times(1)).addSignalParams(any(), any(), anyLong());
    verify(queueProducer, times(1)).push(any(SignalInstance.class));
  }

  @Test
  public void testAddDuplicateSignal() throws Exception {
    SignalCreateRequest request =
        loadObject("fixtures/api/sample-signal-create-request.json", SignalCreateRequest.class);
    var instance = brokerDao.addSignal(request);
    assertEquals("signal_a", instance.getName());
    assertEquals("test-request-id", instance.getInstanceId());
    assertEquals(0, instance.getSeqId());
    assertNotNull(instance.getDetails());
    assertEquals("Duplicate signal instance", instance.getDetails().getMessage());
    verify(instanceDao, times(1)).addSignalInstance(any(), any(), any(), any());
    verify(paramDao, times(0)).addSignalParams(any(), any(), anyLong());
    verify(queueProducer, times(0)).push(any(SignalInstance.class));
  }

  @Test
  public void testMatchSignalForStepDependencyWithParams() {
    SignalMatchDto matchDto =
        new SignalMatchDto(
            "signal_a",
            List.of(
                new SignalMatchDto.ParamMatchDto(
                    "foo", SignalParamValue.of(123), SignalOperator.EQUALS_TO)));
    brokerDao.matchSignalForStepDependency(matchDto);
    verify(paramDao, times(1)).matchSignalDependency(matchDto);
    verify(instanceDao, times(0)).matchSignalDependency(matchDto);
  }

  @Test
  public void testMatchSignalForStepDependencyWithoutParams() {
    SignalMatchDto matchDto = new SignalMatchDto("signal_a", null);
    brokerDao.matchSignalForStepDependency(matchDto);
    verify(paramDao, times(0)).matchSignalDependency(matchDto);
    verify(instanceDao, times(1)).matchSignalDependency(matchDto);
  }

  @Test
  public void testSubscribeWorkflowTriggers() throws Exception {
    when(triggerDao.addWorkflowTrigger(any(), any(), any(), any())).thenReturn(true);
    brokerDao.subscribeWorkflowTriggers(
        conn,
        List.of(
            new SignalTriggerDef("test-wf1", "uuid1", null),
            new SignalTriggerDef("test-wf2", "uuid2", null)));
    verify(triggerDao, times(2)).addWorkflowTrigger(any(), any(), any(), any());
  }

  @Test(expected = MaestroResourceConflictException.class)
  public void testSubscribeWorkflowTriggersWithError() throws Exception {
    when(triggerDao.addWorkflowTrigger(any(), any(), any(), any())).thenReturn(false);
    brokerDao.subscribeWorkflowTriggers(
        conn, List.of(new SignalTriggerDef("test-wf1", "uuid1", null)));
    verify(triggerDao, times(2)).addWorkflowTrigger(any(), any(), any(), any());
  }

  @Test
  public void testGetSubscribedTriggers() throws Exception {
    SignalInstance instance =
        loadObject("fixtures/signal/sample-signal-instance.json", SignalInstance.class);
    when(triggerDao.getSubscribedTriggers("signal_a"))
        .thenReturn(List.of(new SignalTriggerMatch()));
    var actual = brokerDao.getSubscribedTriggers(instance);
    assertEquals(1, actual.size());
    assertEquals(instance, actual.getFirst().getSignalInstance());
  }

  @Test
  public void testDeleteTrigger() {
    brokerDao.deleteTrigger("test-wf1", "uuid1");
    verify(triggerDao, times(1)).delete(any(), any());
  }

  @Test
  public void testGetSignalInstances() {
    var actual =
        brokerDao.getSignalInstances(
            List.of(new SignalInstanceRef("foo", 1), new SignalInstanceRef("bar", 2)));
    assertEquals(2, actual.size());
    verify(instanceDao, times(2)).getSignalInstance(any());
  }

  @Test
  public void testGetSignalInstance() {
    brokerDao.getSignalInstance("foo", 1);
    verify(instanceDao, times(1)).getSignalInstance(any());
  }

  @Test
  public void testGetLatestSignalInstance() {
    brokerDao.getLatestSignalInstance("foo");
    verify(instanceDao, times(1)).getSignalInstance(any());
  }

  @Test
  public void testTryExecuteTriggerIfNotFound() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    assertEquals(-1, brokerDao.tryExecuteTrigger(triggerMatch));
  }

  @Test
  public void testTryExecuteTriggerIfInstanceInvalid() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    when(triggerDao.getTriggerForUpdate(any(), any(), any()))
        .thenReturn(
            new SignalTriggerDto(
                "sample-workflow-1",
                "test-uuid-1",
                "{}",
                new String[] {"signal_a", "signal_b"},
                new Long[] {1L, 2L}));
    assertEquals(0, brokerDao.tryExecuteTrigger(triggerMatch));
  }

  @Test
  public void testTryExecuteTriggerIfNotMatch() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    when(triggerDao.getTriggerForUpdate(any(), any(), any()))
        .thenReturn(
            new SignalTriggerDto(
                "sample-workflow-1",
                "test-uuid-1",
                "{}",
                new String[] {"signal_a", "signal_b"},
                new Long[] {0L, 2L}));
    assertEquals(0, brokerDao.tryExecuteTrigger(triggerMatch));
  }

  @Test
  public void testTryExecuteTriggerForUnmatched() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    String def = loadJson("fixtures/signal_triggers/signal_trigger_simple.json");
    when(triggerDao.getTriggerForUpdate(any(), any(), any()))
        .thenReturn(
            new SignalTriggerDto(
                "sample-workflow-1",
                "test-uuid-1",
                def,
                new String[] {"signal_a", "signal_b"},
                new Long[] {0L, 2L}));
    when(paramDao.matchSignal(any(), any(), anyLong())).thenReturn(null);
    assertEquals(0, brokerDao.tryExecuteTrigger(triggerMatch));
  }

  @Test
  public void testTryExecuteTrigger() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    String def = loadJson("fixtures/signal_triggers/signal_trigger_simple.json");
    when(triggerDao.getTriggerForUpdate(any(), any(), any()))
        .thenReturn(
            new SignalTriggerDto(
                "sample-workflow-1",
                "test-uuid-1",
                def,
                new String[] {"signal_a", "signal_b"},
                new Long[] {0L, 2L}));
    when(triggerDao.updateTriggerCheckpoints(any(), any(), any(), any())).thenReturn(true);
    assertEquals(1, brokerDao.tryExecuteTrigger(triggerMatch));
    verify(queueProducer, times(1)).push(any(SignalTriggerExecution.class));
  }

  @Test
  public void testTryExecuteTriggerWithError() throws Exception {
    SignalTriggerMatch triggerMatch =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    String def = loadJson("fixtures/signal_triggers/signal_trigger_simple.json");
    when(triggerDao.getTriggerForUpdate(any(), any(), any()))
        .thenReturn(
            new SignalTriggerDto(
                "sample-workflow-1",
                "test-uuid-1",
                def,
                new String[] {"signal_a", "signal_b"},
                new Long[] {0L, 2L}));
    AssertHelper.assertThrows(
        "Failed to update the checkpoint",
        MaestroRetryableError.class,
        "Failed to update the checkpoint for",
        () -> brokerDao.tryExecuteTrigger(triggerMatch));
    verify(queueProducer, times(0)).push(any(SignalTriggerExecution.class));
  }
}
