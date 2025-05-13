/*
 * Copyright 2025 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.listeners.SqsSignalInstanceListener;
import com.netflix.maestro.engine.listeners.SqsSignalTriggerExecutionListener;
import com.netflix.maestro.engine.listeners.SqsSignalTriggerMatchListener;
import com.netflix.maestro.engine.listeners.SqsTimeTriggerExecutionListener;
import com.netflix.maestro.engine.producer.SqsSignalQueueProducer;
import com.netflix.maestro.engine.producer.SqsTimeTriggerProducer;
import com.netflix.maestro.engine.properties.AwsProperties;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.publisher.SnsEventNotificationPublisher;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.signal.messageprocessors.SignalInstanceProcessor;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerExecutionProcessor;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerMatchProcessor;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.maestro.timetrigger.messageprocessors.TimeTriggerExecutionProcessor;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@Slf4j
@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class AwsConfiguration {
  /** qualifier for Maestro Aws SQS Sync Publishers bean. */
  private static final String MAESTRO_AWS_SQS_SYNC = "maestroAwsSqsSync";

  /** create event notification client wrapper. */
  @Bean
  @ConditionalOnProperty(value = "maestro.notifier.type", havingValue = "sns")
  public MaestroNotificationPublisher notificationPublisher(
      SnsClient amazonSns,
      AwsProperties props,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro notificationPublisher within Spring boot...");
    return new SnsEventNotificationPublisher(amazonSns, props.getSnsTopic(), objectMapper);
  }

  /** create sqs template. */
  @Bean(MAESTRO_AWS_SQS_SYNC)
  @ConditionalOnProperty(value = "triggers.time-trigger.type", havingValue = "sqs")
  public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
    return SqsTemplate.builder().sqsAsyncClient(sqsAsyncClient).build();
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.time-trigger.type", havingValue = "sqs")
  public TimeTriggerProducer sqsTimeTriggerProducer(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) SqsTemplate amazonSqs,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      AwsProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating sqsTimeTriggerProducer within Spring boot...");
    return new SqsTimeTriggerProducer(amazonSqs, objectMapper, props.getSqs(), metrics);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.time-trigger.type", havingValue = "sqs")
  public SqsTimeTriggerExecutionListener sqsTimeTriggerExecutionListener(
      TimeTriggerExecutionProcessor timeTriggerExecutionProcessor, ObjectMapper mapper) {
    LOG.info("Creating sqsTimeTriggerExecutionListener within Spring boot...");
    return new SqsTimeTriggerExecutionListener(timeTriggerExecutionProcessor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.signal-trigger.type", havingValue = "sqs")
  public SignalQueueProducer sqsSignalQueueProducer(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) SqsTemplate amazonSqs,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      AwsProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating sqsSignalQueueProducer within Spring boot...");
    return new SqsSignalQueueProducer(amazonSqs, objectMapper, props.getSqs(), metrics);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.signal-trigger.type", havingValue = "sqs")
  public SqsSignalInstanceListener sqsSignalInstanceListener(
      SignalInstanceProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalInstanceListener within Spring boot...");
    return new SqsSignalInstanceListener(processor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.signal-trigger.type", havingValue = "sqs")
  public SqsSignalTriggerMatchListener sqsSignalTriggerMatchListener(
      SignalTriggerMatchProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalTriggerMatchListener within Spring boot...");
    return new SqsSignalTriggerMatchListener(processor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.signal-trigger.type", havingValue = "sqs")
  public SqsSignalTriggerExecutionListener sqsSignalTriggerExecutionListener(
      SignalTriggerExecutionProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalTriggerExecutionListener within Spring boot...");
    return new SqsSignalTriggerExecutionListener(processor, mapper);
  }

  /** SqsAsyncClient has already been created by springboot sqs autoconfiguration. */
  @Bean
  @ConditionalOnProperty(value = "maestro.time-trigger.type", havingValue = "sqs")
  public SqsMessageListenerContainerFactory<Object> simpleMessageListenerContainerFactory(
      SqsAsyncClient amazonSqs, AwsProperties props) {
    LOG.info("Creating simpleMessageListenerContainerFactory within Spring boot...");
    return SqsMessageListenerContainerFactory.builder()
        .configure(
            options -> {
              options.componentsTaskExecutor(createDefaultTaskExecutor(props.getSqs()));
              options.maxConcurrentMessages(props.getSqs().getListenerMaxNumberOfMessages());
              options.pollTimeout(Duration.ofMillis(props.getSqs().getListenerWaitTimeoutInSecs()));
              options.autoStartup(true);
              options.acknowledgementMode(AcknowledgementMode.ON_SUCCESS);
            })
        .sqsAsyncClient(amazonSqs)
        .build();
  }

  private AsyncTaskExecutor createDefaultTaskExecutor(SqsProperties props) {
    LOG.info("Creating asyncTaskExecutor within Spring boot...");
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix("MaestroSqsExecutor-");
    threadPoolTaskExecutor.setCorePoolSize(props.getListenerCorePoolSize());
    threadPoolTaskExecutor.setMaxPoolSize(props.getListenerMaxPoolSize());
    threadPoolTaskExecutor.setQueueCapacity(props.getListenerQueueCapacity());
    threadPoolTaskExecutor.afterPropertiesSet();
    return threadPoolTaskExecutor;
  }
}
