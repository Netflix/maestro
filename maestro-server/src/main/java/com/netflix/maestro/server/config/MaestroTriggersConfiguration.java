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

import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.server.properties.TriggersProperties;
import com.netflix.maestro.timetrigger.messageprocessors.TimeTriggerExecutionProcessor;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import com.netflix.maestro.timetrigger.utils.MaestroWorkflowLauncher;
import com.netflix.maestro.timetrigger.utils.TimeTriggerExecutionPlanner;
import com.netflix.maestro.timetrigger.utils.TimeTriggerSubscriptionClient;
import lombok.extern.slf4j.Slf4j;
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
  public TriggerSubscriptionClient triggerSubscriptionClient(
      TimeTriggerProducer producer, MaestroMetrics metrics) {
    LOG.info("Creating triggerSubscriptionClient within Spring boot...");
    return new TimeTriggerSubscriptionClient(producer, metrics);
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
}
