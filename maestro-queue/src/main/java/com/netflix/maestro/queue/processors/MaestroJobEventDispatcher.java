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
package com.netflix.maestro.queue.processors;

import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import java.util.EnumMap;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible for routing the job events to the appropriate event processor based on
 * the event type.
 */
@AllArgsConstructor
@Slf4j
public class MaestroJobEventDispatcher {
  private final EnumMap<MaestroJobEvent.Type, MaestroEventProcessor<? extends MaestroJobEvent>>
      jobEventProcessors;

  /**
   * Process a maestro job event by calling the actual job event processing based on the event type.
   *
   * @param jobEvent maestro job event to process
   */
  @SuppressWarnings("unchecked")
  public <T extends MaestroJobEvent> Optional<MaestroJobEvent> processJobEvent(T jobEvent) {
    LOG.info("process a maestro job event: [{}]", jobEvent);
    var processor = (MaestroEventProcessor<T>) jobEventProcessors.get(jobEvent.getType());
    if (processor == null) {
      LOG.warn("no processor found for job event [{}] and ignore it", jobEvent);
      return Optional.empty();
    }
    return processor.process(jobEvent);
  }

  public Set<MaestroJobEvent.Type> typesSupported() {
    return jobEventProcessors.keySet();
  }
}
