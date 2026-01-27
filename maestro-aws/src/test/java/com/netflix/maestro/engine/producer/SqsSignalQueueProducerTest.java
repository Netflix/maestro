package com.netflix.maestro.engine.producer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalTriggerExecution;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.spectator.api.DefaultRegistry;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for SqsSignalQueueProducer class.
 *
 * @author jun-he
 */
public class SqsSignalQueueProducerTest extends MaestroBaseTest {
  private SqsTemplate amazonSqs;
  private MaestroMetricRepo metricRepo;

  private SqsSignalQueueProducer signalTriggerProducer;
  private SignalInstance signalInstance;
  private SignalTriggerMatch signalTriggerMatch;
  private SignalTriggerExecution signalTriggerExecution;

  @Before
  public void setup() {
    amazonSqs = mock(SqsTemplate.class);
    SqsProperties sqsProperties = new SqsProperties();
    sqsProperties.setSignalInstanceQueueUrl("signal-instance-queue-url");
    sqsProperties.setSignalTriggerMatchQueueUrl("signal-trigger-match-queue-url");
    sqsProperties.setSignalTriggerExecutionQueueUrl("signal-trigger-execution-queue-url");

    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    signalTriggerProducer =
        new SqsSignalQueueProducer(amazonSqs, MAPPER, sqsProperties, metricRepo);

    signalInstance = new SignalInstance();
    signalTriggerMatch = new SignalTriggerMatch();
    signalTriggerExecution = new SignalTriggerExecution();
  }

  @Test
  public void testPushSignalInstance() {
    signalTriggerProducer.push(signalInstance);
    verify(amazonSqs, times(1)).send(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_SUCCESS_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalInstance")
            .count());
  }

  @Test
  public void testPushSignalInstanceWithError() {
    when(amazonSqs.send(any())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Should throw the error",
        RuntimeException.class,
        "java.lang.RuntimeException: test",
        () -> signalTriggerProducer.push(signalInstance));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_FAILURE_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalInstance")
            .count());
  }

  @Test
  public void testPushSignalTriggerMatch() {
    signalTriggerProducer.push(signalTriggerMatch);
    verify(amazonSqs, times(1)).send(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_SUCCESS_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalTriggerMatch")
            .count());
  }

  @Test
  public void testPushSignalTriggerMatchWithError() {
    when(amazonSqs.send(any())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Should throw the error",
        RuntimeException.class,
        "java.lang.RuntimeException: test",
        () -> signalTriggerProducer.push(signalTriggerMatch));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_FAILURE_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalTriggerMatch")
            .count());
  }

  @Test
  public void testPushSignalTriggerExecution() {
    signalTriggerProducer.push(signalTriggerExecution);
    verify(amazonSqs, times(1)).send(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_SUCCESS_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalTriggerExecution")
            .count());
  }

  @Test
  public void testPushSignalTriggerExecutionWithError() {
    when(amazonSqs.send(any())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Should throw the error",
        RuntimeException.class,
        "java.lang.RuntimeException: test",
        () -> signalTriggerProducer.push(signalTriggerExecution));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_SIGNAL_PUBLISH_FAILURE_METRIC,
                SqsSignalQueueProducer.class,
                "type",
                "SignalTriggerExecution")
            .count());
  }
}
