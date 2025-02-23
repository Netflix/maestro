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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.listeners.SqsDeleteWorkflowJobListener;
import com.netflix.maestro.engine.listeners.SqsPublishJobEventListener;
import com.netflix.maestro.engine.listeners.SqsRunWorkflowInstancesJobListener;
import com.netflix.maestro.engine.listeners.SqsStartWorkflowJobListener;
import com.netflix.maestro.engine.listeners.SqsStepInstanceWakeUpJobListener;
import com.netflix.maestro.engine.listeners.SqsTerminateInstancesJobListener;
import com.netflix.maestro.engine.listeners.SqsTerminateThenRunInstanceJobListener;
import com.netflix.maestro.engine.listeners.SqsTimeTriggerExecutionListener;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.ExceptionEventDeletionPolicy;
import com.netflix.maestro.engine.processors.PublishJobEventProcessor;
import com.netflix.maestro.engine.processors.RunWorkflowInstancesJobProcessor;
import com.netflix.maestro.engine.processors.SqsProcessorFinalizer;
import com.netflix.maestro.engine.processors.StartWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.StepInstanceWakeUpEventProcessor;
import com.netflix.maestro.engine.processors.TerminateInstancesJobProcessor;
import com.netflix.maestro.engine.processors.TerminateThenRunInstanceJobProcessor;
import com.netflix.maestro.engine.producer.SqsTimeTriggerProducer;
import com.netflix.maestro.engine.properties.AwsProperties;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.publisher.SnsEventNotificationPublisher;
import com.netflix.maestro.engine.publisher.SqsMaestroJobEventPublisher;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.timetrigger.messageprocessors.TimeTriggerExecutionProcessor;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.aws.core.region.RegionProvider;
import org.springframework.cloud.aws.messaging.config.SimpleMessageListenerContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Slf4j
@Configuration
@EnableConfigurationProperties(AwsProperties.class)
public class AwsConfiguration {
  /** qualifier for Maestro Aws SQS Sync Publishers bean. */
  private static final String MAESTRO_AWS_SQS_SYNC = "maestroAwsSqsSync";

  /** create sns. */
  @Primary
  @Bean
  @ConditionalOnProperty(value = "maestro.notifier.type", havingValue = "sns")
  public AmazonSNS amazonSns(
      AWSCredentialsProvider awsCredentialsProvider,
      RegionProvider regionProvider,
      AwsProperties props) {
    LOG.info("Creating Maestro amazonSns within Spring boot...");
    if (props.getSns().endpoint() == null) {
      return AmazonSNSClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withRegion(regionProvider.getRegion().getName())
          .build();
    } else {
      return AmazonSNSClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  props.getSns().endpoint(), regionProvider.getRegion().getName()))
          .build();
    }
  }

  /** create event notification client wrapper. */
  @Bean
  @ConditionalOnProperty(value = "maestro.notifier.type", havingValue = "sns")
  public MaestroNotificationPublisher notificationPublisher(
      AmazonSNS amazonSns,
      AwsProperties props,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro notificationPublisher within Spring boot...");
    return new SnsEventNotificationPublisher(amazonSns, props.getSns().topic(), objectMapper);
  }

  /** create sync sqs. */
  @Bean(MAESTRO_AWS_SQS_SYNC)
  @ConditionalOnProperty(value = "maestro.publisher.type", havingValue = "sqs")
  public AmazonSQS amazonSqsForPublisher(
      AWSCredentialsProvider awsCredentialsProvider, RegionProvider regionProvider) {
    LOG.info("Creating Maestro amazonSQSForPublisher within Spring boot...");
    return AmazonSQSClientBuilder.standard()
        .withRegion(regionProvider.getRegion().getName())
        .withCredentials(awsCredentialsProvider)
        .build();
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.publisher.type", havingValue = "sqs")
  public MaestroJobEventPublisher sqsMaestroJobEventPublisher(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) AmazonSQS amazonSqs,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      AwsProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating sqsMaestroJobEventPublisher within Spring boot...");
    return new SqsMaestroJobEventPublisher(amazonSqs, objectMapper, props.getSqs(), metrics);
  }

  @Bean
  @ConditionalOnProperty(value = "triggers.time-trigger.type", havingValue = "sqs")
  public TimeTriggerProducer sqsTimeTriggerProducer(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) AmazonSQS amazonSqs,
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
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsProcessorFinalizer sqsProcessorFinalizer(
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroMetricRepo metricRepo) {
    LOG.info("Creating sqsProcessorFinalizer within Spring boot...");
    return new SqsProcessorFinalizer(
        objectMapper, metricRepo, ExceptionEventDeletionPolicy.DELETE_IF_MAESTRO_INTERNAL_ERROR);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsRunWorkflowInstancesJobListener sqsRunWorkflowInstancesJobListener(
      RunWorkflowInstancesJobProcessor runWorkflowInstancesJobProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating runWorkflowInstancesJobListener within Spring boot...");
    return new SqsRunWorkflowInstancesJobListener(
        runWorkflowInstancesJobProcessor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsStartWorkflowJobListener sqsStartWorkflowJobListener(
      StartWorkflowJobProcessor startWorkflowJobProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsStartWorkflowJobListener within Spring boot...");
    return new SqsStartWorkflowJobListener(startWorkflowJobProcessor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsTerminateInstancesJobListener sqsTerminateInstancesJobListener(
      TerminateInstancesJobProcessor terminateInstancesJobProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsTerminateInstancesJobListener within Spring boot...");
    return new SqsTerminateInstancesJobListener(
        terminateInstancesJobProcessor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsTerminateThenRunInstanceJobListener sqsTerminateThenRunInstanceJobListener(
      TerminateThenRunInstanceJobProcessor terminateThenRunInstanceJobProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsTerminateThenRunInstanceJobListener within Spring boot...");
    return new SqsTerminateThenRunInstanceJobListener(
        terminateThenRunInstanceJobProcessor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsStepInstanceWakeUpJobListener sqsStepInstanceActionJobListener(
      StepInstanceWakeUpEventProcessor processor, SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsStepInstanceActionJobListener within Spring boot...");
    return new SqsStepInstanceWakeUpJobListener(processor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsPublishJobEventListener sqsPublishJobEventListener(
      PublishJobEventProcessor publishJobEventProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsPublishJobEventListener within Spring boot...");
    return new SqsPublishJobEventListener(publishJobEventProcessor, sqsProcessorFinalizer);
  }

  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SqsDeleteWorkflowJobListener sqsDeleteWorkflowJobListener(
      DeleteWorkflowJobProcessor deleteWorkflowJobProcessor,
      SqsProcessorFinalizer sqsProcessorFinalizer) {
    LOG.info("Creating sqsDeleteWorkflowJobListener within Spring boot...");
    return new SqsDeleteWorkflowJobListener(deleteWorkflowJobProcessor, sqsProcessorFinalizer);
  }

  /** AmazonSQSAsync has already been created by springboot aws autoconfiguration . */
  @Bean
  @ConditionalOnProperty(value = "maestro.listener.type", havingValue = "sqs")
  public SimpleMessageListenerContainerFactory simpleMessageListenerContainerFactory(
      AmazonSQSAsync amazonSqs, AwsProperties props) {
    LOG.info("Creating simpleMessageListenerContainerFactory within Spring boot...");
    SimpleMessageListenerContainerFactory factory = new SimpleMessageListenerContainerFactory();
    factory.setAmazonSqs(amazonSqs);
    factory.setTaskExecutor(createDefaultTaskExecutor(props.getSqs()));
    factory.setMaxNumberOfMessages(props.getSqs().getListenerMaxNumberOfMessages());
    factory.setWaitTimeOut(props.getSqs().getListenerWaitTimeoutInSecs());
    factory.setAutoStartup(true);
    return factory;
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
