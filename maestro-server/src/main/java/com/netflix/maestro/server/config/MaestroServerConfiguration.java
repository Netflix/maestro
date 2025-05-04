/*
 * Copyright 2024 Netflix, Inc.
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
package com.netflix.maestro.server.config;

import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobEventProcessor;
import com.netflix.maestro.engine.processors.InstanceActionJobEventProcessor;
import com.netflix.maestro.engine.processors.NotificationJobEventProcessor;
import com.netflix.maestro.engine.processors.StartWorkflowJobEventProcessor;
import com.netflix.maestro.engine.processors.TerminateThenRunJobEventProcessor;
import com.netflix.maestro.engine.processors.UpdateJobEventProcessor;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.publisher.NoOpMaestroNotificationPublisher;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import com.netflix.maestro.queue.processors.MaestroJobEventDispatcher;
import com.netflix.maestro.queue.worker.MaestroQueueWorkerService;
import com.netflix.maestro.server.interceptor.UserInfoInterceptor;
import com.netflix.maestro.server.properties.MaestroProperties;
import java.util.EnumMap;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** beans for maestro server related classes. */
@Configuration
@Slf4j
@EnableConfigurationProperties(MaestroProperties.class)
public class MaestroServerConfiguration {
  private static final String EVENT_QUEUES_QUALIFIER = "EventQueues";

  @Bean
  public UserInfoInterceptor userInfoInterceptor(User.UserBuilder callerBuilder) {
    LOG.info("Creating UserInfoInterceptor via spring boot...");
    return new UserInfoInterceptor(callerBuilder);
  }

  @ConditionalOnProperty(
      value = "maestro.notifier.type",
      havingValue = "noop",
      matchIfMissing = true)
  @Bean
  public MaestroNotificationPublisher notificationPublisher() {
    LOG.info("Creating NoOp MaestroNotificationPublisher within Spring boot...");
    return new NoOpMaestroNotificationPublisher();
  }

  // below are beans for internal queue and processors.
  @Bean
  public StartWorkflowJobEventProcessor startWorkflowJobProcessor(
      MaestroWorkflowDao workflowDao,
      MaestroRunStrategyDao runStrategyDao,
      MaestroWorkflowInstanceDao instanceDao,
      WorkflowRunner workflowRunner) {
    LOG.info("Creating startWorkflowJobProcessor within Spring boot...");
    return new StartWorkflowJobEventProcessor(
        workflowDao, runStrategyDao, instanceDao, workflowRunner);
  }

  @Bean
  public TerminateThenRunJobEventProcessor terminateThenRunInstanceJobProcessor(
      MaestroWorkflowInstanceDao maestroWorkflowInstanceDao,
      MaestroStepInstanceActionDao maestroStepInstanceActionDao,
      WorkflowRunner workflowRunner) {
    LOG.info("Creating terminateThenRunInstanceJobProcessor within Spring boot...");
    return new TerminateThenRunJobEventProcessor(
        maestroWorkflowInstanceDao, maestroStepInstanceActionDao, workflowRunner);
  }

  @Bean
  public InstanceActionJobEventProcessor instanceActionJobEventProcessor(
      FlowOperation flowOperation,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao) {
    LOG.info("Creating instanceActionJobEventProcessor within Spring boot...");
    return new InstanceActionJobEventProcessor(flowOperation, instanceDao, stepInstanceDao);
  }

  @Bean
  public UpdateJobEventProcessor publishJobEventProcessor(
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      MaestroStepInstanceActionDao maestroStepInstanceActionDao,
      FlowOperation flowOperation,
      MaestroQueueSystem queueSystem) {
    LOG.info("Creating publishJobEventProcessor within Spring boot...");
    return new UpdateJobEventProcessor(
        TagPermitManager.NOOP_TAG_PERMIT_MANAGER,
        instanceStepConcurrencyHandler,
        maestroStepInstanceActionDao,
        flowOperation,
        queueSystem);
  }

  @Bean
  public NotificationJobEventProcessor notificationJobEventProcessor(
      MaestroNotificationPublisher eventClient,
      @Value("${maestro.cluster.name}") String clusterName) {
    LOG.info("Creating notificationJobEventProcessor within Spring boot...");
    return new NotificationJobEventProcessor(eventClient, clusterName);
  }

  @Bean
  public DeleteWorkflowJobEventProcessor deleteWorkflowJobProcessor(
      MaestroWorkflowDeletionDao workflowDataDeletionDao,
      @Value("${maestro.cluster.name}") String clusterName) {
    LOG.info(
        "Creating deleteWorkflowJobProcessor for cluster [{}] within Spring boot...", clusterName);
    return new DeleteWorkflowJobEventProcessor(workflowDataDeletionDao, clusterName);
  }

  @Bean
  public MaestroJobEventDispatcher maestroJobEventDispatcher(
      StartWorkflowJobEventProcessor startWorkflowJobEventProcessor,
      TerminateThenRunJobEventProcessor terminateThenRunJobEventProcessor,
      InstanceActionJobEventProcessor instanceActionJobEventProcessor,
      UpdateJobEventProcessor updateJobEventProcessor,
      NotificationJobEventProcessor notificationJobEventProcessor,
      DeleteWorkflowJobEventProcessor deleteWorkflowJobEventProcessor) {
    LOG.info("Creating maestroJobEventDispatcher within Spring boot...");
    EnumMap<MaestroJobEvent.Type, MaestroEventProcessor<? extends MaestroJobEvent>>
        eventProcessorMap = new EnumMap<>(MaestroJobEvent.Type.class);
    eventProcessorMap.put(MaestroJobEvent.Type.START_WORKFLOW, startWorkflowJobEventProcessor);
    eventProcessorMap.put(
        MaestroJobEvent.Type.TERMINATE_THEN_RUN, terminateThenRunJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.INSTANCE_ACTION, instanceActionJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.STEP_INSTANCE_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.WORKFLOW_INSTANCE_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.WORKFLOW_VERSION_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.NOTIFICATION, notificationJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.DELETE_WORKFLOW, deleteWorkflowJobEventProcessor);
    return new MaestroJobEventDispatcher(eventProcessorMap);
  }

  @Bean(destroyMethod = "shutdown")
  public MaestroQueueWorkerService maestroQueueWorkerService(
      @Qualifier(EVENT_QUEUES_QUALIFIER)
          EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues,
      MaestroJobEventDispatcher maestroJobEventDispatcher,
      MaestroQueueDao queueDao,
      MaestroProperties properties,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroQueueWorkerService within Spring boot...");
    return new MaestroQueueWorkerService(
        eventQueues, maestroJobEventDispatcher, queueDao, properties.getQueue(), metrics);
  }

  @Bean(destroyMethod = "shutdown")
  public MaestroQueueSystem maestroQueueSystem(
      @Qualifier(EVENT_QUEUES_QUALIFIER)
          EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues,
      MaestroQueueDao queueDao,
      MaestroProperties properties,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroQueueSystem within Spring boot...");
    return new MaestroQueueSystem(eventQueues, queueDao, properties.getQueue(), metrics);
  }

  @Bean(name = EVENT_QUEUES_QUALIFIER)
  public EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues() {
    LOG.info("Creating eventQueues within Spring boot...");
    return MaestroJobEvent.createQueuesForJobEvents();
  }
}
