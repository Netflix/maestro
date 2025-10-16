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

import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import com.netflix.maestro.utils.TriggerHelper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utility class to plan time trigger executions. */
public class TimeTriggerExecutionPlanner {

  private final int maxFutureItems;

  /**
   * Constructor.
   *
   * @param maxFutureItems limit for calculating future runs.
   */
  public TimeTriggerExecutionPlanner(int maxFutureItems) {
    this.maxFutureItems = maxFutureItems;
  }

  /**
   * Calculate executions for time triggers.
   *
   * @param timeTriggerWithWatermarks an array of time triggers with watermark
   * @param workflowId used by fuzzy cron as salt to calculate random jitter
   * @return a list of planned executions
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "PMD.ReplaceJavaUtilDate"})
  public List<PlannedTimeTriggerExecution> calculatePlannedExecutions(
      List<TimeTriggerWithWatermark> timeTriggerWithWatermarks, Date endDate, String workflowId) {

    List<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    for (TimeTriggerWithWatermark t : timeTriggerWithWatermarks) {
      Optional<Date> nextExecutionDate =
          TriggerHelper.nextExecutionDate(
              t.getTimeTrigger(), new Date(t.getLastTriggerTimestamp()), workflowId);
      while (plannedList.size() < maxFutureItems
          && nextExecutionDate.isPresent()
          && endDate.compareTo(nextExecutionDate.get()) > 0) {
        plannedList.add(new PlannedTimeTriggerExecution(t, nextExecutionDate.get()));
        nextExecutionDate =
            TriggerHelper.nextExecutionDate(
                t.getTimeTrigger(), nextExecutionDate.get(), workflowId);
      }
    }
    plannedList.sort(Comparator.comparing(PlannedTimeTriggerExecution::executionDate));
    return plannedList;
  }

  /**
   * Calculate earliest execution date for time triggers.
   *
   * @param timeTriggers an array of time triggers with watermark
   * @return the earliest date
   */
  @SuppressWarnings("PMD.ReplaceJavaUtilDate")
  public Optional<Date> calculateEarliestExecutionDate(
      List<TimeTriggerWithWatermark> timeTriggers, String workflowId) {
    return timeTriggers.stream()
        .map(
            t ->
                TriggerHelper.nextExecutionDate(
                    t.getTimeTrigger(), new Date(t.getLastTriggerTimestamp()), workflowId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .min(Comparator.naturalOrder());
  }

  /**
   * Construct next time trigger execution message from existing message and executed time triggers.
   *
   * @param timeTriggerExecution the existing time trigger execution from the message
   * @param executedTimeTriggers the list of executed time triggers
   * @return the next time trigger execution message
   */
  public TimeTriggerExecution constructNextExecution(
      TimeTriggerExecution timeTriggerExecution,
      List<PlannedTimeTriggerExecution> executedTimeTriggers) {
    var timeTriggerWithWatermarks = timeTriggerExecution.getTimeTriggersWithWatermarks();
    Map<TimeTrigger, Instant> latestExecutedByTrigger = new HashMap<>();

    executedTimeTriggers.forEach(
        t -> {
          if (latestExecutedByTrigger.containsKey(t.timeTriggerWithWatermark().getTimeTrigger())) {
            Instant executionInstant =
                latestExecutedByTrigger.get(t.timeTriggerWithWatermark().getTimeTrigger());
            if (t.executionDate().toInstant().compareTo(executionInstant) > 0) {
              latestExecutedByTrigger.put(
                  t.timeTriggerWithWatermark().getTimeTrigger(), t.executionDate().toInstant());
            }
          } else {
            latestExecutedByTrigger.put(
                t.timeTriggerWithWatermark().getTimeTrigger(), t.executionDate().toInstant());
          }
        });

    var updatedTriggers =
        timeTriggerWithWatermarks.stream()
            .map(
                t -> {
                  if (latestExecutedByTrigger.containsKey(t.getTimeTrigger())) {
                    return TimeTriggerWithWatermark.builder()
                        .timeTrigger(t.getTimeTrigger())
                        .lastTriggerTimestamp(
                            latestExecutedByTrigger.get(t.getTimeTrigger()).toEpochMilli())
                        .build();
                  } else {
                    return t;
                  }
                })
            .toList();
    return TimeTriggerExecution.builder()
        .workflowTriggerUuid(timeTriggerExecution.getWorkflowTriggerUuid())
        .workflowVersion(timeTriggerExecution.getWorkflowVersion())
        .workflowId(timeTriggerExecution.getWorkflowId())
        .timeTriggersWithWatermarks(updatedTriggers)
        .build();
  }
}
