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
package com.netflix.maestro.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalTriggerExecution;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.server.properties.TriggersProperties;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.messageprocessors.SignalInstanceProcessor;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerExecutionProcessor;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerMatchProcessor;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.maestro.signal.utils.SignalTriggerSubscriptionClient;
import com.netflix.maestro.timetrigger.messageprocessors.TimeTriggerExecutionProcessor;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import com.netflix.maestro.timetrigger.utils.MaestroWorkflowLauncher;
import com.netflix.maestro.timetrigger.utils.TimeTriggerExecutionPlanner;
import com.netflix.maestro.timetrigger.utils.TimeTriggerSubscriptionClient;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for maestro trigger classes. */
@SuppressWarnings("JavadocMethod")
@Configuration
@Slf4j
@EnableConfigurationProperties({TriggersProperties.class})
public class MaestroTriggersConfiguration {
  @Bean
  public MaestroWorkflowLauncher maestroWorkflowLauncher(WorkflowActionHandler actionHandler) {
    LOG.info("Creating maestroWorkflowLauncher within Spring boot...");
    return new MaestroWorkflowLauncher(actionHandler);
  }

  @Bean
  public TimeTriggerExecutionPlanner timeTriggerExecutionPlanner(TriggersProperties properties) {
    LOG.info("Creating timeTriggerExecutionPlanner within Spring boot...");
    return new TimeTriggerExecutionPlanner(properties.getTimeTrigger().getMaxTriggersPerMessage());
  }

  @Bean
  @ConditionalOnProperty(
      value = "triggers.time-trigger.type",
      havingValue = "noop",
      matchIfMissing = true)
  public TimeTriggerProducer noopTimeTriggerProducer() {
    LOG.info("Creating noopTimeTriggerProducer within Spring boot...");
    return (execution, delay) ->
        LOG.info("[NoOp] push time trigger execution [{}] with delay [{}]", execution, delay);
  }

  @Bean
  public TimeTriggerExecutionProcessor timeTriggerExecutionProcessor(
      TimeTriggerProducer timeTriggerProducer,
      MaestroWorkflowLauncher maestroWorkflowLauncher,
      TimeTriggerExecutionPlanner executionPlanner,
      TriggersProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating timeTriggerExecutionProcessor within Spring boot...");
    return new TimeTriggerExecutionProcessor(
        timeTriggerProducer,
        maestroWorkflowLauncher,
        executionPlanner,
        props.getTimeTrigger(),
        metrics);
  }

  @Bean
  public TriggerSubscriptionClient triggerSubscriptionClient(
      MaestroSignalBrokerDao brokerDao, TimeTriggerProducer producer, MaestroMetrics metrics) {
    LOG.info("Creating both signal and time triggerSubscriptionClient within Spring boot...");
    return new TriggerSubscriptionClient() {
      private final TriggerSubscriptionClient signalTriggerClient =
          new SignalTriggerSubscriptionClient(brokerDao, metrics);
      private final TriggerSubscriptionClient timeTriggerClient =
          new TimeTriggerSubscriptionClient(producer, metrics);

      @Override
      public void upsertTriggerSubscription(
          Connection conn, Workflow workflow, TriggerUuids current, TriggerUuids previous)
          throws SQLException {
        signalTriggerClient.upsertTriggerSubscription(conn, workflow, current, previous);
        timeTriggerClient.upsertTriggerSubscription(conn, workflow, current, previous);
      }
    };
  }

  // Below are signal related beans.
  @Bean
  @ConditionalOnProperty(
      value = "triggers.signal-trigger.type",
      havingValue = "noop",
      matchIfMissing = true)
  public SignalQueueProducer noopSignalQueueProducer() {
    LOG.info("Creating noopSignalQueueProducer within Spring boot...");
    return new SignalQueueProducer() {
      @Override
      public void push(SignalInstance signalInstance) {}

      @Override
      public void push(SignalTriggerMatch triggerMatch) {}

      @Override
      public void push(SignalTriggerExecution triggerExecution) {}
    };
  }

  @Bean
  public SignalInstanceProcessor signalInstanceProcessor(
      MaestroSignalBrokerDao brokerDao, SignalQueueProducer producer, MaestroMetrics metrics) {
    LOG.info("Creating signalInstanceProcessor within Spring boot...");
    return new SignalInstanceProcessor(brokerDao, producer, metrics);
  }

  @Bean
  public SignalTriggerMatchProcessor signalTriggerMatchProcessor(
      MaestroSignalBrokerDao brokerDao, MaestroMetrics metrics) {
    LOG.info("Creating signalTriggerMatchProcessor within Spring boot...");
    return new SignalTriggerMatchProcessor(brokerDao, metrics);
  }

  @Bean
  public SignalTriggerExecutionProcessor signalTriggerExecutionProcessor(
      MaestroSignalBrokerDao brokerDao,
      ParamEvaluator paramEvaluator,
      WorkflowActionHandler actionHandler,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroMetrics metrics) {
    LOG.info("Creating signalTriggerExecutionProcessor within Spring boot...");
    return new SignalTriggerExecutionProcessor(
        brokerDao, paramEvaluator, actionHandler, objectMapper, metrics);
  }
}
