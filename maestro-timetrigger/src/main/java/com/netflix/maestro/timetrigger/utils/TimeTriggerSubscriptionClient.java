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
package com.netflix.maestro.timetrigger.utils;

import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.timetrigger.Constants;
import com.netflix.maestro.timetrigger.metrics.MetricConstants;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Time trigger subscription client. */
@Slf4j
@AllArgsConstructor
public class TimeTriggerSubscriptionClient implements TriggerSubscriptionClient {
  private final TimeTriggerProducer triggerProducer;
  private final MaestroMetrics metrics;

  @Override
  public void upsertTriggerSubscription(
      Workflow workflow, TriggerUuids current, TriggerUuids previous) {
    if (workflow.getTimeTriggers() != null
        && !workflow.getTimeTriggers().isEmpty()
        && current != null
        && current.getTimeTriggerUuid() != null) {
      if (previous == null
          || !Objects.equals(current.getTimeTriggerUuid(), previous.getTimeTriggerUuid())) {
        LOG.info(
            "Update time trigger [{}] for workflow id [{}]",
            current.getTimeTriggerUuid(),
            workflow.getId());
        TimeTriggerExecution execution = convertToExecution(workflow, current);
        insertSubscription(execution);
      } else {
        LOG.info(
            "No time trigger update for workflow id [{}] as it has been sent.", workflow.getId());
      }
    }
  }

  private void insertSubscription(TimeTriggerExecution execution) {
    // Create initial execution message
    triggerProducer.push(execution, Constants.MESSAGE_DELAY_FIRST_EXECUTION);
    metrics.counter(MetricConstants.CREATE_SUBSCRIPTION_METRIC, getClass());
  }

  /** Convert subscription to execution message. */
  private TimeTriggerExecution convertToExecution(Workflow workflow, TriggerUuids current) {
    var timeTriggerWithWatermarks =
        workflow.getTimeTriggers().stream()
            .map(
                t ->
                    TimeTriggerWithWatermark.builder()
                        .lastTriggerTimestamp(System.currentTimeMillis())
                        .timeTrigger(t)
                        .build())
            .toList();
    return TimeTriggerExecution.builder()
        .workflowId(workflow.getId())
        .workflowVersion(Constants.TIME_TRIGGER_WORKFLOW_VERSION)
        .workflowTriggerUuid(current.getTimeTriggerUuid())
        .timeTriggersWithWatermarks(timeTriggerWithWatermarks)
        .build();
  }
}
