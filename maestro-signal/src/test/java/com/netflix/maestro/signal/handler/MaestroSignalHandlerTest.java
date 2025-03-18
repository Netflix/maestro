package com.netflix.maestro.signal.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for MaestroSignalHandler class.
 *
 * @author jun-he
 */
public class MaestroSignalHandlerTest extends MaestroBaseTest {
  private MaestroSignalBrokerDao brokerDao;
  private MaestroSignalHandler handler;

  @Before
  public void setup() {
    brokerDao = mock(MaestroSignalBrokerDao.class);
    handler = new MaestroSignalHandler(brokerDao);
  }

  @Test
  public void testSendOutputSignals() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-with-dynamic-output.json",
            StepRuntimeSummary.class);
    runtimeSummary.initializeSignalOutputs(
        null,
        null,
        runtimeSummary
            .getArtifacts()
            .get(Artifact.Type.DYNAMIC_OUTPUT.key())
            .asDynamicOutput()
            .getSignalOutputs());
    SignalInstance instance = new SignalInstance();
    instance.setSeqId(3);
    when(brokerDao.addSignal(any())).thenReturn(instance);
    assertTrue(handler.sendOutputSignals(new WorkflowSummary(), runtimeSummary));
    assertEquals(
        3, runtimeSummary.getSignalOutputs().getOutputs().getFirst().getSignalId().intValue());
    assertEquals(
        3, runtimeSummary.getSignalOutputs().getOutputs().getLast().getSignalId().intValue());
    assertEquals(
        "Signal step outputs have been completed.",
        runtimeSummary.getSignalOutputs().getInfo().getMessage());
    assertFalse(runtimeSummary.isSynced());
    verify(brokerDao, times(2)).addSignal(any());
  }

  @Test
  public void testSendOutputSignalsWithError() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-with-dynamic-output.json",
            StepRuntimeSummary.class);
    runtimeSummary.initializeSignalOutputs(
        null,
        null,
        runtimeSummary
            .getArtifacts()
            .get(Artifact.Type.DYNAMIC_OUTPUT.key())
            .asDynamicOutput()
            .getSignalOutputs());

    AssertHelper.assertThrows(
        "Retry error for output signal",
        MaestroRetryableError.class,
        "error when sending output signal",
        () -> handler.sendOutputSignals(new WorkflowSummary(), runtimeSummary));
  }

  @Test
  public void testSignalReady() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/step-runtime-summary-with-step-dependencies.json",
            StepRuntimeSummary.class);
    when(brokerDao.matchSignalForStepDependency(any())).thenReturn(Long.valueOf(3));
    assertTrue(handler.signalsReady(new WorkflowSummary(), runtimeSummary));
    verify(brokerDao, times(1)).matchSignalForStepDependency(any());
    assertEquals(
        StepDependencyMatchStatus.MATCHED,
        runtimeSummary.getSignalDependencies().getDependencies().getLast().getStatus());
    assertFalse(runtimeSummary.isSynced());
  }

  @Test
  public void testSignalReadyFalse() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/step-runtime-summary-with-step-dependencies.json",
            StepRuntimeSummary.class);
    when(brokerDao.matchSignalForStepDependency(any())).thenReturn(null);
    assertFalse(handler.signalsReady(new WorkflowSummary(), runtimeSummary));
    verify(brokerDao, times(1)).matchSignalForStepDependency(any());
    assertEquals(
        StepDependencyMatchStatus.PENDING,
        runtimeSummary.getSignalDependencies().getDependencies().getLast().getStatus());
    assertTrue(runtimeSummary.isSynced());
  }

  @Test
  public void testGetSignalInstance() throws Exception {
    SignalInstance instance = new SignalInstance();
    instance.setSeqId(3);
    instance.setParams(Map.of("foo", SignalParamValue.of("bar")));
    when(brokerDao.getSignalInstance(any(), anyLong())).thenReturn(instance);
    var actual = handler.getSignalInstance("signal_a", 123);
    assertEquals(instance, actual);
    verify(brokerDao, times(1)).getSignalInstance(any(), anyLong());
  }
}
