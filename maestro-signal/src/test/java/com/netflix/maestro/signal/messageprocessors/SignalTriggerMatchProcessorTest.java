package com.netflix.maestro.signal.messageprocessors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for SignalTriggerMatchProcessor class.
 *
 * @author jun-he
 */
public class SignalTriggerMatchProcessorTest {
  private MaestroSignalBrokerDao brokerDao;
  private MaestroMetricRepo metricRepo;
  private SignalTriggerMatchProcessor processor;

  @Before
  public void setup() {
    brokerDao = mock(MaestroSignalBrokerDao.class);
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    processor = new SignalTriggerMatchProcessor(brokerDao, metricRepo);
  }

  @Test
  public void testProcessWithExecute() {
    SignalTriggerMatch instance = new SignalTriggerMatch();
    Supplier<SignalTriggerMatch> messageSupplier = () -> instance;
    when(brokerDao.tryExecuteTrigger(any())).thenReturn(1);
    processor.process(messageSupplier);
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_MATCH_SUCCESS,
                SignalTriggerMatchProcessor.class,
                MetricConstants.TYPE_TAG,
                "1")
            .count());
  }

  @Test
  public void testProcessForInvalidMatch() {
    SignalTriggerMatch instance = new SignalTriggerMatch();
    Supplier<SignalTriggerMatch> messageSupplier = () -> instance;
    when(brokerDao.tryExecuteTrigger(any())).thenReturn(-1);
    processor.process(messageSupplier);
    verify(brokerDao, times(1)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_MATCH_SUCCESS,
                SignalTriggerMatchProcessor.class,
                MetricConstants.TYPE_TAG,
                "-1")
            .count());
  }

  @Test
  public void testProcessWithRetryableError() {
    SignalTriggerMatch instance = new SignalTriggerMatch();
    Supplier<SignalTriggerMatch> messageSupplier = () -> instance;
    when(brokerDao.tryExecuteTrigger(any())).thenThrow(new MaestroRetryableError("foo"));
    AssertHelper.assertThrows(
        "retryable error",
        MaestroRetryableError.class,
        "foo",
        () -> processor.process(messageSupplier));
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_MATCH_FAILURE,
                SignalTriggerMatchProcessor.class,
                MetricConstants.TYPE_TAG,
                "retryable")
            .count());
  }

  @Test
  public void testProcessWithRuntimeError() {
    SignalTriggerMatch instance = new SignalTriggerMatch();
    Supplier<SignalTriggerMatch> messageSupplier = () -> instance;
    when(brokerDao.tryExecuteTrigger(any())).thenThrow(new RuntimeException("foo"));
    AssertHelper.assertThrows(
        "recoverable error",
        MaestroRetryableError.class,
        "will retry the error for",
        () -> processor.process(messageSupplier));
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_MATCH_FAILURE,
                SignalTriggerMatchProcessor.class,
                MetricConstants.TYPE_TAG,
                "recoverable")
            .count());
  }
}
