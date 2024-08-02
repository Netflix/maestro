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
package com.netflix.maestro.engine.execution;

import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;

/**
 * Step synchronization manager to write the update to maestro step instance table and also publish
 * the events to the internal queue.
 */
public final class StepSyncManager {
  private final MaestroStepInstanceDao instanceDao;
  private final MaestroJobEventPublisher jobEventPublisher;

  /** Step synchronization manager constructor. */
  public StepSyncManager(
      MaestroStepInstanceDao instanceDao, MaestroJobEventPublisher jobEventPublisher) {
    this.instanceDao = instanceDao;
    this.jobEventPublisher = jobEventPublisher;
  }

  /**
   * sync the pending updates to maestro step instance db and also publish notifications.
   *
   * <p>It never throws an exception and step runtime state machine will call sync again if it fails
   * and offers at-least once guarantee.
   *
   * @param instance It is used for the first time sync to create step instance in DB.
   * @param workflowSummary workflow summary including workflow id and instance id info
   * @param stepSummary runtime step summary includes the pending updates
   * @return exception details if sync fails, otherwise empty.
   */
  public Optional<Details> sync(
      @NotNull StepInstance instance,
      @NotNull WorkflowSummary workflowSummary,
      @NotNull StepRuntimeSummary stepSummary) {
    try {
      switch (stepSummary.getDbOperation()) {
        case INSERT:
        case UPSERT:
          instanceDao.insertOrUpsertStepInstance(
              instance, stepSummary.getDbOperation() == DbOperation.UPSERT);
          break;
        case UPDATE:
          instanceDao.updateStepInstance(workflowSummary, stepSummary);
          break;
        default:
          throw new MaestroInternalError(
              "Invalid DB operation: %s for step instance [%s][%s]",
              stepSummary.getDbOperation(),
              stepSummary.getStepId(),
              stepSummary.getStepAttemptId());
      }
      if (!stepSummary.getPendingRecords().isEmpty()) {
        return jobEventPublisher.publish(
            StepInstanceUpdateJobEvent.create(instance, stepSummary.getPendingRecords()));
      }
      return Optional.empty();
    } catch (RuntimeException e) {
      return Optional.of(Details.create(e, true, "Failed to sync a Maestro step state change"));
    }
  }
}
