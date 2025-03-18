package com.netflix.maestro.signal.messageprocessors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for SignalInstanceProcessor class.
 *
 * @author jun-he
 */
public class SignalInstanceProcessorTest {
  private MaestroSignalBrokerDao brokerDao;
  private SignalQueueProducer producer;
  private MaestroMetricRepo metricRepo;
  private SignalInstanceProcessor processor;

  @Before
  public void setup() {
    brokerDao = mock(MaestroSignalBrokerDao.class);
    producer = mock(SignalQueueProducer.class);
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    processor = new SignalInstanceProcessor(brokerDao, producer, metricRepo);
  }

  @Test
  public void testProcessWithMatch() {
    SignalInstance instance = new SignalInstance();
    instance.setSeqId(12);
    Supplier<SignalInstance> messageSupplier = () -> instance;
    when(brokerDao.getSubscribedTriggers(any())).thenReturn(List.of(new SignalTriggerMatch()));
    processor.process(messageSupplier);
    verify(producer, times(1)).push(any(SignalTriggerMatch.class));
    assertEquals(
        1L,
        metricRepo
            .getCounter(MetricConstants.SIGNAL_TRIGGER_MATCH_FOUND, SignalInstanceProcessor.class)
            .count());
  }

  @Test
  public void testProcessWithoutMatch() {
    SignalInstance instance = new SignalInstance();
    instance.setSeqId(12);
    Supplier<SignalInstance> messageSupplier = () -> instance;
    when(brokerDao.getSubscribedTriggers(any())).thenReturn(List.of());
    processor.process(messageSupplier);
    verify(producer, times(0)).push(any(SignalTriggerMatch.class));
    assertEquals(
        0L,
        metricRepo
            .getCounter(MetricConstants.SIGNAL_TRIGGER_MATCH_FOUND, SignalInstanceProcessor.class)
            .count());
  }

  @Test
  public void testProcessInvalidSignalInstance() {
    Supplier<SignalInstance> messageSupplier = SignalInstance::new;
    AssertHelper.assertThrows(
        "Invalid seq id",
        IllegalArgumentException.class,
        "it must be positive",
        () -> processor.process(messageSupplier));
  }
}
