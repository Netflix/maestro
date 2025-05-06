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
package com.netflix.maestro.queue.models;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import java.util.Map;

/**
 * The message data transfer object.
 *
 * @param ownedUntil the time this message is owned by the worker
 * @param msgId message identifier, it won't be used for deduplication
 * @param event if the event is null, this message is a system operational message. message id
 *     infers the type.
 * @param createTime the time this message was created
 */
public record MessageDto(
    long ownedUntil, String msgId, @Nullable MaestroJobEvent event, long createTime) {
  /** The constant for the system operational message. */
  public static final MessageDto SCAN_CMD_MSG =
      new MessageDto(Long.MAX_VALUE, "maestro-queue-scan", null, System.currentTimeMillis());

  public static MessageDto createMessageForWakeUp(
      String workflowId, long groupInfo, Map<Long, Long> instanceRunIds) {
    var jobEvent = InstanceActionJobEvent.create(workflowId, groupInfo, instanceRunIds);
    return new MessageDto(
        Long.MAX_VALUE, jobEvent.getIdentity(), jobEvent, System.currentTimeMillis());
  }

  /** Get the event type from the event. */
  public MaestroJobEvent.Type type() {
    if (isInternal()) {
      return null;
    }
    return event.getType();
  }

  /** Get queue id from the event. */
  public int queueId() {
    if (isInternal()) {
      return 0;
    }
    return event.getType().getQueueId();
  }

  public boolean isInternal() {
    return event == null;
  }

  public boolean inMemory() {
    return ownedUntil == Long.MAX_VALUE;
  }
}
