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
package com.netflix.maestro.queue.processors;

import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import java.util.Optional;

/**
 * EventProcessor processes the event of type T.
 *
 * <p>While implementing this interface make sure that any exceptions that are thrown from the
 * {@link MaestroEventProcessor#process(T)} code as properly handled so that they can either be
 * retried or deleted.
 *
 * <p>Suggest to throw {@link com.netflix.maestro.exceptions.MaestroInternalError} and {@link
 * com.netflix.maestro.exceptions.MaestroRetryableError} for error handling.
 *
 * @param <T>
 */
public interface MaestroEventProcessor<T extends MaestroJobEvent> {
  /**
   * Processes the message of type T. After processing, it is optionally to return a MaestroJobEvent
   * for the next processing, which might be a different type of MaestroJobEvent. The queue system
   * guarantee to finish the current job event and enqueue this next event in a transaction.
   *
   * @return an optional next event to process.
   */
  Optional<MaestroJobEvent> process(T jobEvent);
}
