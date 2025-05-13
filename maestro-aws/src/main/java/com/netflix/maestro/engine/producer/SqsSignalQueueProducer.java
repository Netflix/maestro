package com.netflix.maestro.engine.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.metrics.MetricConstants;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.maestro.utils.IdHelper;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Producer for Signal related messages.
 *
 * @author jun-he
 */
@AllArgsConstructor
@Slf4j
public class SqsSignalQueueProducer implements SignalQueueProducer {
  private static final String KEY_FORMAT = "[%s][%s]";
  private final SqsTemplate amazonSqs;
  private final ObjectMapper objectMapper;
  private final SqsProperties props;
  private final MaestroMetrics metrics;

  @Override
  public void push(SignalInstance signalInstance) {
    LOG.debug("Publishing signal instance: [{}]", signalInstance);
    send(signalInstance, props.getSignalInstanceQueueUrl(), signalInstance.getName());
  }

  @Override
  public void push(SignalTriggerMatch triggerMatch) {
    LOG.debug("Publishing signal trigger match: [{}]", triggerMatch);
    send(
        triggerMatch,
        props.getSignalTriggerMatchQueueUrl(),
        String.format(KEY_FORMAT, triggerMatch.getWorkflowId(), triggerMatch.getTriggerUuid()));
  }

  @Override
  public void push(SignalTriggerExecution triggerExecution) {
    LOG.debug("Publishing signal trigger execution: [{}]", triggerExecution);
    send(
        triggerExecution,
        props.getSignalTriggerExecutionQueueUrl(),
        String.format(
            KEY_FORMAT, triggerExecution.getWorkflowId(), triggerExecution.getTriggerUuid()));
  }

  /** Send a signal related message. */
  private void send(Object message, String queueUrl, String groupKey) {
    try {
      String strRequest = objectMapper.writeValueAsString(message);
      amazonSqs.send(
          sqsSendOptions ->
              sqsSendOptions
                  .queue(queueUrl)
                  .payload(strRequest)
                  .messageGroupId(groupKey)
                  .messageDeduplicationId(IdHelper.createUuid(strRequest).toString()));
      metrics.counter(
          AwsMetricConstants.SQS_SIGNAL_PUBLISH_SUCCESS_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          message.getClass().getSimpleName());
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.SQS_SIGNAL_PUBLISH_FAILURE_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          message.getClass().getSimpleName());
      LOG.error("Error when sending the message [{}] with exception: ", message, e);
      throw new RuntimeException(e);
    }
  }
}
