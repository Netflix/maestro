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
package com.netflix.maestro.engine.publisher;

import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.error.Details;
import java.util.Optional;

/** Maestro job event publisher responsible for only publishing {@link MaestroJobEvent}. */
public interface MaestroJobEventPublisher {
  /** Default invisible time for an event. */
  long DEFAULT_INVISIBLE_TIME = 0L;

  /**
   * Publish maestro job event to the internal job queue.
   *
   * <p>If failed, it should not throw an exception instead using putting errors to Details.
   *
   * @param jobEvent maestro job event to publish
   * @return empty means no error and published. Otherwise, it carries the error details.
   */
  default Optional<Details> publish(MaestroJobEvent jobEvent) {
    return publish(jobEvent, DEFAULT_INVISIBLE_TIME);
  }

  /**
   * Publish maestro job event to the internal job queue with a delay specified by invisible time.
   *
   * <p>If failed, it should not throw an exception instead using putting errors to Details.
   *
   * @param jobEvent maestro job event to publish
   * @param invisibleMs invisible time in milliseconds
   * @return empty means no error and published. Otherwise, it carries the error details.
   */
  Optional<Details> publish(MaestroJobEvent jobEvent, long invisibleMs);

  /**
   * Publish maestro job event to the internal job queue.
   *
   * @param jobEvent maestro job event to publish
   * @throws MaestroRetryableError if failed to publish the job event.
   */
  default void publishOrThrow(MaestroJobEvent jobEvent) {
    publishOrThrow(jobEvent, "Failed sending " + jobEvent + ", retry it");
  }

  /**
   * Publish maestro job event to the internal job queue.
   *
   * @param jobEvent maestro job event to publish
   * @throws MaestroRetryableError if failed to publish the job event.
   */
  default void publishOrThrow(MaestroJobEvent jobEvent, String msg) {
    publishOrThrow(jobEvent, DEFAULT_INVISIBLE_TIME, msg);
  }

  /**
   * Publish maestro job event to the internal job queue with a delay specified by invisible time.
   *
   * @param jobEvent maestro job event to publish
   * @param invisibleMs invisible time in milliseconds
   * @throws MaestroRetryableError if failed to publish the job event.
   */
  default void publishOrThrow(MaestroJobEvent jobEvent, long invisibleMs, String msg) {
    Optional<Details> error = publish(jobEvent, invisibleMs);
    if (error.isPresent()) {
      throw new MaestroRetryableError(error.get(), msg);
    }
  }
}
