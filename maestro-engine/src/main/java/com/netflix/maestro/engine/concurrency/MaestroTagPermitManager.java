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
package com.netflix.maestro.engine.concurrency;

import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.tasks.MaestroTagPermitTask;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.List;
import java.util.UUID;

/** Database based implementation of tag permit manager. */
public class MaestroTagPermitManager implements TagPermitManager {
  private static final int NOT_FOUND_STATUS_CODE = -1;

  private final MaestroTagPermitDao tagPermitDao;
  private final MaestroQueueSystem queueSystem;

  /** Constructor for DbTagPermitManager. */
  public MaestroTagPermitManager(MaestroTagPermitDao tagPermitDao, MaestroQueueSystem queueSystem) {
    this.tagPermitDao = tagPermitDao;
    this.queueSystem = queueSystem;
  }

  /**
   * Async acquire permits for every tag in tagList for a given step uuid. Expect caller to
   * periodically call this method until success.
   *
   * @param tagsList list of tags to acquire permits for.
   * @param stepUuid step uuid.
   * @return status of acquisition.
   */
  @Override
  public Status acquire(List<Tag> tagsList, String stepUuid, TimelineEvent event) {
    if (ObjectHelper.isCollectionEmptyOrNull(tagsList)) {
      return new Status(true, "No tags associated with the step, skip the tag permit acquisition");
    }
    UUID uuid = UUID.fromString(stepUuid);
    int statusCode = tagPermitDao.getStepTagPermitStatus(uuid);
    if (statusCode == NOT_FOUND_STATUS_CODE) {
      boolean res = tagPermitDao.insertStepTagPermit(tagsList, uuid, event);
      if (res) {
        wakeUpTagPermitTask(MaestroTagPermitTask.STEP_TAG_PERMIT_CHANGE_CODE);
      }
      statusCode = 0;
    }
    boolean status = statusCode == MaestroTagPermitTask.ACQUIRED_STATUS_CODE;
    return new Status(
        status, status ? "Tag permit acquired" : "The step is still waiting for tag permit");
  }

  private void wakeUpTagPermitTask(int code) {
    var msg =
        MessageDto.createMessageForWakeUp(
            0, Constants.INTERNAL_FLOW_NAME, Constants.TAG_PERMIT_TASK_NAME, code);
    queueSystem.notify(msg);
  }

  /**
   * Releases the tag permits and returns the list of released tags. Expect caller to periodically
   * call this method until success.
   *
   * <p>It will wake up the tag permit handler to re-check the queue with the best effort. If the
   * async release is missed, either reconciliation or another release will fix it.
   *
   * @param stepUuid step uuid.
   */
  @Override
  public void releaseTagPermits(String stepUuid) {
    UUID uuid = UUID.fromString(stepUuid);
    boolean released = tagPermitDao.releaseStepTagPermit(uuid);
    if (released) {
      wakeUpTagPermitTask(MaestroTagPermitTask.STEP_TAG_PERMIT_DELETE_CODE);
    }
  }

  /**
   * Create or update the tag permit limit requested by a user.
   *
   * <p>If the async update is failed, either reconciliation or another update will fix it.
   *
   * @param tag tag name
   * @param limit tag permit limit
   * @param user user performing the operation
   */
  @Override
  public void upsertTagPermit(String tag, int limit, User user) {
    tagPermitDao.upsertTagPermit(tag, limit, user);
    wakeUpTagPermitTask(MaestroTagPermitTask.TAG_PERMIT_CHANGE_CODE);
  }

  /**
   * Remove the tag permit asynchronously.
   *
   * @param tag tag name
   */
  @Override
  public void removeTagPermit(String tag) {
    boolean removed = tagPermitDao.markRemoveTagPermit(tag);
    if (removed) {
      wakeUpTagPermitTask(MaestroTagPermitTask.TAG_PERMIT_DELETE_CODE);
    }
  }

  /**
   * Get the tag permit information.
   *
   * @param tag tag name
   * @return tag permit information
   */
  @Override
  public TagPermit getTagPermit(String tag) {
    return tagPermitDao.getTagPermit(tag);
  }
}
