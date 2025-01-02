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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.MaestroWorkflowExecutor;
import com.netflix.conductor.core.execution.MaestroWorkflowTaskRunner;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.conductor.core.execution.mapper.ExclusiveJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.JoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper;
import com.netflix.conductor.core.execution.tasks.SystemTaskWorkerCoordinator;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.DummyPayloadStorage;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.core.utils.Lock;
import com.netflix.conductor.core.utils.NoopLock;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.service.ExecutionLockService;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.maestro.engine.dao.InMemoryQueueDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.listeners.MaestroWorkflowStatusListener;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.server.properties.ConductorProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for conductor library related classes. */
@Slf4j
@Configuration
@EnableConfigurationProperties(ConductorProperties.class)
public class ConductorConfiguration {
  static final String CONDUCTOR_QUALIFIER = "conductor";

  @Bean(name = CONDUCTOR_QUALIFIER)
  public ObjectMapper objectMapper() {
    LOG.info("Creating conductor objectMapper within Spring boot...");
    return new JsonMapperProvider().get();
  }

  @Bean
  @ConditionalOnProperty(
      value = "maestro.queue.type",
      havingValue = "in-memory",
      matchIfMissing = true)
  public QueueDAO queueDAO() {
    LOG.info("Creating memory based queueDAO within Spring boot...");
    return new InMemoryQueueDao(new ConcurrentHashMap<>());
  }

  @Bean
  public MaestroWorkflowExecutor workflowExecutor(
      DeciderService deciderService,
      MetadataDAO metadataDAO,
      QueueDAO queueDAO,
      MetadataMapperService metadataMapperService,
      WorkflowStatusListener workflowStatusListener,
      ExecutionDAOFacade executionDAOFacade,
      ConductorProperties props,
      ExecutionLockService executionLockService,
      ParametersUtils parametersUtils,
      StepRuntimeCallbackDelayPolicy stepRuntimeCallbackDelayPolicy,
      MaestroWorkflowTaskRunner maestroWorkflowTaskRunner,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating MaestroWorkflowExecutor within Spring boot...");
    return new MaestroWorkflowExecutor(
        deciderService,
        metadataDAO,
        queueDAO,
        metadataMapperService,
        workflowStatusListener,
        executionDAOFacade,
        props,
        executionLockService,
        parametersUtils,
        stepRuntimeCallbackDelayPolicy,
        maestroWorkflowTaskRunner,
        objectMapper);
  }

  @Bean
  public DeciderService deciderService(
      ParametersUtils parametersUtils,
      MetadataDAO metadataDAO,
      ExternalPayloadStorageUtils externalPayloadStorageUtils,
      @Qualifier("TaskMappers") Map<String, TaskMapper> taskMappers,
      ConductorProperties props) {
    LOG.info("Creating deciderService within Spring boot...");
    return new DeciderService(
        parametersUtils, metadataDAO, externalPayloadStorageUtils, taskMappers, props);
  }

  @Bean
  public MetadataMapperService metadataMapperService(MetadataDAO metadataDAO) {
    LOG.info("Creating metadataMapperService within Spring boot...");
    return new MetadataMapperService(metadataDAO);
  }

  @Bean
  public WorkflowStatusListener workflowStatusListener(
      MaestroTask maestroTask,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroJobEventPublisher publisher,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroMetrics metricRepo) {
    LOG.info("Creating workflowStatusListener within Spring boot...");
    return new MaestroWorkflowStatusListener(
        maestroTask, instanceDao, stepInstanceDao, publisher, objectMapper, metricRepo);
  }

  @Bean
  public ExecutionDAOFacade executionDAOFacade(
      ExecutionDAO executionDAO,
      QueueDAO queueDAO,
      IndexDAO indexDAO,
      RateLimitingDAO rateLimitingDao,
      PollDataDAO pollDataDAO,
      @Qualifier(CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating executionDAOFacade within Spring boot...");
    return new ExecutionDAOFacade(
        executionDAO, queueDAO, indexDAO, rateLimitingDao, pollDataDAO, objectMapper, props);
  }

  @Bean
  public ExecutionLockService executionLockService(
      ConductorProperties props, Provider<Lock> lockProvider) {
    LOG.info("Creating executionLockService within Spring boot...");
    return new ExecutionLockService(props, lockProvider);
  }

  @Bean
  public ParametersUtils parametersUtils() {
    LOG.info("Creating noop parametersUtils within Spring boot...");
    return new ParametersUtils() {
      @Override
      public Map<String, Object> getTaskInputV2(
          Map<String, Object> input, Workflow workflow, String taskId, TaskDef taskDefinition) {
        return input;
      }
    };
  }

  @Bean
  @ConditionalOnProperty(
      value = "maestro.redis.enabled",
      havingValue = "false",
      matchIfMissing = true)
  public Lock noopLock() {
    LOG.info("Creating noop lock within Spring boot...");
    return new NoopLock();
  }

  @Bean
  public Provider<Lock> lockProvider(Lock lock) {
    LOG.info("Creating Lock Provider within Spring boot...");
    return () -> lock;
  }

  @Bean
  public MaestroWorkflowTaskRunner maestroWorkflowTaskRunner() {
    LOG.info("Creating maestroWorkflowTaskRunner within Spring boot...");
    return new MaestroWorkflowTaskRunner();
  }

  @Bean
  public ExternalPayloadStorageUtils externalPayloadStorageUtils(
      ExternalPayloadStorage externalPayloadStorage,
      ConductorProperties props,
      @Qualifier(CONDUCTOR_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating externalPayloadStorageUtils within Spring boot...");
    return new ExternalPayloadStorageUtils(externalPayloadStorage, props, objectMapper);
  }

  @Bean
  public ExternalPayloadStorage externalPayloadStorage() {
    LOG.info("Creating externalPayloadStorage within Spring boot...");
    return new DummyPayloadStorage();
  }

  @Bean(name = "TaskMappers")
  public Map<String, TaskMapper> taskMapperMap(
      ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
    LOG.info("Creating taskMapperMap within Spring boot...");
    Map<String, TaskMapper> mappers = new HashMap<>();
    mappers.put(TaskType.TASK_TYPE_JOIN, new JoinTaskMapper());
    mappers.put(TaskType.TASK_TYPE_FORK_JOIN, new ForkJoinTaskMapper());
    mappers.put(TaskType.TASK_TYPE_EXCLUSIVE_JOIN, new ExclusiveJoinTaskMapper());
    mappers.put(
        TaskType.TASK_TYPE_USER_DEFINED, new UserDefinedTaskMapper(parametersUtils, metadataDAO));
    return Collections.unmodifiableMap(mappers);
  }

  @Bean
  public SystemTaskWorkerCoordinator systemTaskWorkerCoordinator(
      QueueDAO queueDAO,
      WorkflowExecutor workflowExecutor,
      ConductorProperties props,
      ExecutionService executionService) {
    LOG.info("Creating systemTaskWorkerCoordinator within Spring boot...");
    return new SystemTaskWorkerCoordinator(queueDAO, workflowExecutor, props, executionService);
  }

  @Bean
  public ExecutionService executionService(
      WorkflowExecutor workflowExecutor,
      ExecutionDAOFacade executionDAOFacade,
      MetadataDAO metadataDAO,
      QueueDAO queueDAO,
      ConductorProperties props,
      ExternalPayloadStorage externalPayloadStorage) {
    LOG.info("Creating executionService within Spring boot...");
    return new ExecutionService(
        workflowExecutor, executionDAOFacade, metadataDAO, queueDAO, props, externalPayloadStorage);
  }
}
