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

import com.netflix.maestro.models.definition.Tag;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

/** Interface for tag permit acquirer. */
public interface TagPermitManager {
  /** A noop tag permit handler. */
  TagPermitManager NOOP_TAG_PERMIT_MANAGER =
      new TagPermitManager() {
        @Override
        public Status acquire(List<Tag> tagsList, String uuid) {
          return new Status(
              true,
              "Using NoOp tag permit acquirer, tag permit feature is disabled and skip the tag permit acquisition");
        }

        @Override
        public List<String> releaseTagPermits(String stepUuid) {
          return Collections.emptyList();
        }
      };

  @Getter
  class Status {
    private final boolean success;
    private final String message;

    public Status(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }

  /** acquire permits for every tag in tagList for a given a uuid (e.g. step uuid). */
  Status acquire(List<Tag> tagsList, String uuid);

  /**
   * Releases the tag permits and returns the list of released tags.
   *
   * @param stepUuid step uuid.
   * @return list of tags released.
   */
  List<String> releaseTagPermits(String stepUuid);
}
