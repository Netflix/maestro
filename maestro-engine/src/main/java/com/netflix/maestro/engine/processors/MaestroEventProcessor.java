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
package com.netflix.maestro.engine.processors;

import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import java.util.function.Supplier;

/**
 * EventProcessor processes the event of type T.
 *
 * <p>While implementing this interface make sure that any exceptions that are thrown from the
 * {@link MaestroEventProcessor#process(Supplier)} code as properly handled so that they can either
 * be retried or deleted.
 *
 * <p>Suggest to throw {@link com.netflix.maestro.exceptions.MaestroInternalError} and {@link
 * com.netflix.maestro.exceptions.MaestroRetryableError} for error handling.
 *
 * @param <T>
 */
public interface MaestroEventProcessor<T extends MaestroJobEvent> {
  /**
   * Processes the message of type T.
   *
   * @param messageSupplier the message supplier
   */
  void process(Supplier<T> messageSupplier);
}
