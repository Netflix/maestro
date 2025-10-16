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
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.RedisInstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.listeners.SqsSignalInstanceListener;
import com.netflix.maestro.engine.listeners.SqsSignalTriggerExecutionListener;
import com.netflix.maestro.engine.listeners.SqsSignalTriggerMatchListener;
import com.netflix.maestro.engine.listeners.SqsTimeTriggerExecutionListener;
import com.netflix.maestro.engine.producer.SqsSignalQueueProducer;
import com.netflix.maestro.engine.producer.SqsTimeTriggerProducer;
import com.netflix.maestro.engine.properties.AwsProperties;
import com.netflix.maestro.engine.properties.RedisProperties;
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
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
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

  private static final String SQS_TYPE = "sqs";
  private static final String TIME_TRIGGER_TYPE_PROPERTY = "triggers.time-trigger.type";
  private static final String SIGNAL_TRIGGER_TYPE_PROPERTY = "triggers.signal-trigger.type";

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
  @ConditionalOnProperty(value = TIME_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
    return SqsTemplate.builder().sqsAsyncClient(sqsAsyncClient).build();
  }

  @Bean
  @ConditionalOnProperty(value = TIME_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public TimeTriggerProducer sqsTimeTriggerProducer(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) SqsTemplate amazonSqs,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      AwsProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating sqsTimeTriggerProducer within Spring boot...");
    return new SqsTimeTriggerProducer(amazonSqs, objectMapper, props.getSqs(), metrics);
  }

  @Bean
  @ConditionalOnProperty(value = TIME_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SqsTimeTriggerExecutionListener sqsTimeTriggerExecutionListener(
      TimeTriggerExecutionProcessor timeTriggerExecutionProcessor, ObjectMapper mapper) {
    LOG.info("Creating sqsTimeTriggerExecutionListener within Spring boot...");
    return new SqsTimeTriggerExecutionListener(timeTriggerExecutionProcessor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = SIGNAL_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SignalQueueProducer sqsSignalQueueProducer(
      @Qualifier(MAESTRO_AWS_SQS_SYNC) SqsTemplate amazonSqs,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      AwsProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating sqsSignalQueueProducer within Spring boot...");
    return new SqsSignalQueueProducer(amazonSqs, objectMapper, props.getSqs(), metrics);
  }

  @Bean
  @ConditionalOnProperty(value = SIGNAL_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SqsSignalInstanceListener sqsSignalInstanceListener(
      SignalInstanceProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalInstanceListener within Spring boot...");
    return new SqsSignalInstanceListener(processor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = SIGNAL_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SqsSignalTriggerMatchListener sqsSignalTriggerMatchListener(
      SignalTriggerMatchProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalTriggerMatchListener within Spring boot...");
    return new SqsSignalTriggerMatchListener(processor, mapper);
  }

  @Bean
  @ConditionalOnProperty(value = SIGNAL_TRIGGER_TYPE_PROPERTY, havingValue = SQS_TYPE)
  public SqsSignalTriggerExecutionListener sqsSignalTriggerExecutionListener(
      SignalTriggerExecutionProcessor processor, ObjectMapper mapper) {
    LOG.info("Creating sqsSignalTriggerExecutionListener within Spring boot...");
    return new SqsSignalTriggerExecutionListener(processor, mapper);
  }

  /** SqsAsyncClient has already been created by springboot sqs autoconfiguration. */
  @Bean
  @ConditionalOnProperty(value = "maestro.time-trigger.type", havingValue = SQS_TYPE)
  public SqsMessageListenerContainerFactory<Object> simpleMessageListenerContainerFactory(
      SqsAsyncClient amazonSqs, AwsProperties props) {
    LOG.info("Creating simpleMessageListenerContainerFactory within Spring boot...");
    return SqsMessageListenerContainerFactory.builder()
        .configure(
            options -> {
              options.componentsTaskExecutor(createDefaultTaskExecutor(props.getSqs()));
              options.maxConcurrentMessages(props.getSqs().getListenerMaxNumberOfMessages());
              options.pollTimeout(
                  Duration.ofSeconds(props.getSqs().getListenerWaitTimeoutInSecs()));
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

  @Bean
  @ConditionalOnProperty(value = "maestro.redis.enabled", havingValue = "true")
  public InstanceStepConcurrencyHandler redisInstanceStepConcurrencyHandler(
      RedissonClient redisson, MaestroMetrics metricRepo) {
    LOG.info("Creating maestro redisInstanceStepConcurrencyHandler within Spring boot...");
    return new RedisInstanceStepConcurrencyHandler(
        redisson.getScript(StringCodec.INSTANCE), metricRepo);
  }

  @Bean(destroyMethod = "shutdown")
  @ConditionalOnProperty(value = "maestro.redis.enabled", havingValue = "true")
  public RedissonClient redisson(AwsProperties props) {
    RedisProperties redisProps = props.getRedis();
    RedisProperties.RedisServerType redisServerType = redisProps.getRedisServerType();

    Config redisConfig = new Config();
    String redisServerAddress = redisProps.getRedisServerAddress();
    int connectionTimeout = redisProps.getRedisConnectionTimeout();
    int scanInterval = redisProps.getRedisScanInterval();

    switch (redisServerType) {
      case CLUSTER:
        redisConfig
            .useClusterServers()
            .setScanInterval(scanInterval) // cluster state scan interval in milliseconds
            .addNodeAddress(redisServerAddress.split(","))
            .setTimeout(connectionTimeout);
        break;
      case SENTINEL:
        redisConfig
            .useSentinelServers()
            .setScanInterval(scanInterval)
            .addSentinelAddress(redisServerAddress)
            .setTimeout(connectionTimeout);
        break;
      default: // for the case of SINGLE
        redisConfig.useSingleServer().setAddress(redisServerAddress).setTimeout(connectionTimeout);
        break;
    }

    return Redisson.create(redisConfig);
  }
}
