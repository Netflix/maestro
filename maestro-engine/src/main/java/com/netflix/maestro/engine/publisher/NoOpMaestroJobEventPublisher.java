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
import com.netflix.maestro.models.error.Details;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** NoOp Maestro job event publisher. */
@Slf4j
@AllArgsConstructor
public class NoOpMaestroJobEventPublisher implements MaestroJobEventPublisher {

  /**
   * Print a maestro job event locally. It should not throw any exception.
   *
   * @param maestroJob maestro job event to publish
   * @return Error details if failed, otherwise empty.
   */
  @Override
  public Optional<Details> publish(MaestroJobEvent maestroJob, long invisibleMs) {
    try {
      LOG.info("Noop publish an maestro job event: {} with delay {}", maestroJob, invisibleMs);
      return Optional.empty();
    } catch (Exception e) {
      return Optional.of(Details.create(e, true, "Failed to publish a Maestro job event"));
    }
  }
}
