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

import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.TimelineEvent;
import java.util.List;

/** Interface for tag permit manager. */
public interface TagPermitManager {
  /** A noop tag permit handler. */
  TagPermitManager NOOP_TAG_PERMIT_MANAGER =
      new TagPermitManager() {
        @Override
        public Status acquire(List<Tag> tagsList, String uuid, TimelineEvent event) {
          return new Status(
              true,
              "Using NoOp tag permit manager, tag permit feature is disabled and skip the tag permit acquisition");
        }

        @Override
        public void releaseTagPermits(String uuid) {}

        @Override
        public void upsertTagPermit(String tag, int limit, User user) {}

        @Override
        public void removeTagPermit(String tag) {}

        @Override
        public TagPermit getTagPermit(String tag) {
          throw new MaestroNotFoundException("No tag permit found for tag " + tag);
        }
      };

  record Status(boolean success, String message) {}

  /**
   * Acquire permits for every tag in tagList for a given an uuid (e.g. step uuid) with a timeline
   * event carrying additional info.
   *
   * @param tagsList list of tags to acquire permits for.
   * @param uuid step uuid.
   * @param event timeline event carrying additional info.
   * @return status of acquisition.
   */
  Status acquire(List<Tag> tagsList, String uuid, TimelineEvent event);

  /**
   * Releases the tag permits held by a step.
   *
   * @param uuid step uuid.
   */
  void releaseTagPermits(String uuid);

  /**
   * Create or update the tag permit limit.
   *
   * @param tag tag name
   * @param limit tag permit limit
   * @param user user performing the operation
   */
  void upsertTagPermit(String tag, int limit, User user);

  /**
   * Remove the tag permit.
   *
   * @param tag tag name
   */
  void removeTagPermit(String tag);

  /**
   * Get the tag permit.
   *
   * @param tag tag name
   */
  TagPermit getTagPermit(String tag);
}
