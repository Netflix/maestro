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
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.listeners.InMemoryJobEventListener;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.PublishJobEventProcessor;
import com.netflix.maestro.engine.processors.RunWorkflowInstancesJobProcessor;
import com.netflix.maestro.engine.processors.StartWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.StepInstanceWakeUpEventProcessor;
import com.netflix.maestro.engine.processors.TerminateInstancesJobProcessor;
import com.netflix.maestro.engine.processors.TerminateThenRunInstanceJobProcessor;
import com.netflix.maestro.engine.publisher.InMemoryMaestroJobEventPublisher;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.publisher.NoOpMaestroNotificationPublisher;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.engine.FlowExecutor;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.server.interceptor.UserInfoInterceptor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** beans for maestro server related classes. */
@Configuration
@Slf4j
public class MaestroServerConfiguration {
  private static final String EVENT_QUEUE_QUALIFIER = "EventPublisherQueue";

  @Bean
  public UserInfoInterceptor userInfoInterceptor(User.UserBuilder callerBuilder) {
    LOG.info("Creating UserInfoInterceptor via spring boot...");
    return new UserInfoInterceptor(callerBuilder);
  }

  @Bean
  public MaestroNotificationPublisher notificationPublisher() {
    LOG.info("Creating NoOp MaestroNotificationPublisher within Spring boot...");
    return new NoOpMaestroNotificationPublisher();
  }

  @Bean
  public DeleteWorkflowJobProcessor deleteWorkflowJobProcessor(
      MaestroWorkflowDeletionDao workflowDataDeletionDao,
      MaestroNotificationPublisher notificationPublisher,
      @Value("${maestro.cluster.name}") String clusterName) {
    LOG.info(
        "Creating deleteWorkflowJobProcessor for cluster [{}] within Spring boot...", clusterName);
    return new DeleteWorkflowJobProcessor(
        workflowDataDeletionDao, notificationPublisher, clusterName);
  }

  @Bean
  public RunWorkflowInstancesJobProcessor runWorkflowInstancesJobProcessor(
      MaestroWorkflowInstanceDao maestroWorkflowInstanceDao,
      MaestroFlowDao flowDao,
      WorkflowRunner workflowRunner) {
    LOG.info("Creating runWorkflowInstancesJobProcessor within Spring boot...");
    return new RunWorkflowInstancesJobProcessor(
        maestroWorkflowInstanceDao, flowDao, workflowRunner);
  }

  @Bean
  public StartWorkflowJobProcessor startWorkflowJobProcessor(
      MaestroWorkflowDao maestroWorkflowDao, MaestroRunStrategyDao maestroRunStrategyDao) {
    LOG.info("Creating startWorkflowJobProcessor within Spring boot...");
    return new StartWorkflowJobProcessor(maestroWorkflowDao, maestroRunStrategyDao);
  }

  @Bean
  public TerminateInstancesJobProcessor terminateInstancesJobProcessor(
      MaestroWorkflowInstanceDao maestroWorkflowInstanceDao,
      MaestroStepInstanceActionDao maestroStepInstanceActionDao) {
    LOG.info("Creating terminateInstancesJobProcessor within Spring boot...");
    return new TerminateInstancesJobProcessor(
        maestroWorkflowInstanceDao, maestroStepInstanceActionDao);
  }

  @Bean
  public TerminateThenRunInstanceJobProcessor terminateThenRunInstanceJobProcessor(
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroWorkflowInstanceDao maestroWorkflowInstanceDao,
      TerminateInstancesJobProcessor terminateInstancesJobProcessor) {
    LOG.info("Creating terminateThenRunInstanceJobProcessor within Spring boot...");
    return new TerminateThenRunInstanceJobProcessor(
        maestroJobEventPublisher, maestroWorkflowInstanceDao, terminateInstancesJobProcessor);
  }

  @Bean
  public StepInstanceWakeUpEventProcessor stepInstanceWakeUpEventProcessor(
      FlowExecutor flowExecutor,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao) {
    LOG.info("Creating stepInstanceWakeUpEventProcessor within Spring boot...");
    return new StepInstanceWakeUpEventProcessor(flowExecutor, instanceDao, stepInstanceDao);
  }

  @Bean
  public PublishJobEventProcessor publishJobEventProcessor(
      WorkflowHelper workflowHelper,
      MaestroNotificationPublisher notificationPublisher,
      MaestroStepInstanceActionDao maestroStepInstanceActionDao,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      @Value("${maestro.cluster.name}") String clusterName) {
    LOG.info(
        "Creating publishJobEventProcessor for cluster [{}] within Spring boot...", clusterName);
    return new PublishJobEventProcessor(
        workflowHelper,
        notificationPublisher,
        TagPermitManager.NOOP_TAG_PERMIT_MANAGER,
        maestroStepInstanceActionDao,
        instanceStepConcurrencyHandler,
        clusterName);
  }

  @Bean
  @ConditionalOnProperty(
      value = "maestro.publisher.type",
      havingValue = "in-memory",
      matchIfMissing = true)
  public MaestroJobEventPublisher inMemoryMaestroJobEventPublisher(
      @Qualifier(EVENT_QUEUE_QUALIFIER) LinkedBlockingQueue<MaestroJobEvent> queue) {
    LOG.info("Creating inMemoryMaestroJobEventPublisher within Spring boot...");
    return new InMemoryMaestroJobEventPublisher(queue);
  }

  @Bean(name = EVENT_QUEUE_QUALIFIER)
  public LinkedBlockingQueue<MaestroJobEvent> queue() {
    LOG.info("Creating LinkedBlockingQueue within Spring boot...");
    return new LinkedBlockingQueue<>();
  }

  @Bean(initMethod = "postConstruct", destroyMethod = "preDestroy")
  public InMemoryJobEventListener inMemoryJobEventListener(
      DeleteWorkflowJobProcessor deleteWorkflowJobProcessor,
      RunWorkflowInstancesJobProcessor runWorkflowInstancesJobProcessor,
      StartWorkflowJobProcessor startWorkflowJobProcessor,
      TerminateInstancesJobProcessor terminateInstancesJobProcessor,
      StepInstanceWakeUpEventProcessor stepInstanceWakeUpEventProcessor,
      TerminateThenRunInstanceJobProcessor terminateThenRunInstanceJobProcessor,
      PublishJobEventProcessor publishJobEventProcessor,
      @Qualifier(EVENT_QUEUE_QUALIFIER) LinkedBlockingQueue<MaestroJobEvent> queue) {
    LOG.info("Creating inMemoryJobEventListener within Spring boot...");
    return new InMemoryJobEventListener(
        deleteWorkflowJobProcessor,
        runWorkflowInstancesJobProcessor,
        startWorkflowJobProcessor,
        terminateInstancesJobProcessor,
        terminateThenRunInstanceJobProcessor,
        stepInstanceWakeUpEventProcessor,
        publishJobEventProcessor,
        queue,
        Executors.newFixedThreadPool(1));
  }
}
