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
package com.netflix.maestro.engine.processors;

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import java.util.function.Function;
import lombok.Getter;

/**
 * The exception event deletion policy specifies which types of exception trigger deletion of the
 * event from the queue.
 */
@Getter
public enum ExceptionEventDeletionPolicy {
  /** Signal service job events queue deletion policy. */
  DELETE_IF_NOT_MAESTRO_RETRYABLE_ERROR((Exception ex) -> !(ex instanceof MaestroRetryableError)),

  /** Maestro core job events queue deletion policy. */
  DELETE_IF_MAESTRO_INTERNAL_ERROR((Exception ex) -> (ex instanceof MaestroInternalError));

  private final Function<Exception, Boolean> policy;

  ExceptionEventDeletionPolicy(Function<Exception, Boolean> policy) {
    this.policy = policy;
  }
}
