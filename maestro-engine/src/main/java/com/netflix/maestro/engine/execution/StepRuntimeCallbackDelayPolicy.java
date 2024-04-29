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
package com.netflix.maestro.engine.execution;

/** Interface to support step runtime callback configurations. */
public interface StepRuntimeCallbackDelayPolicy {
  /** Get callback for the step instance current runtime state. */
  Long getCallBackDelayInSecs(StepRuntimeSummary runtimeSummary);

  /** Get callback type. */
  CallbackDelayPolicyType getType();

  /** Supported callback policy enum. */
  enum CallbackDelayPolicyType {
    /** Fixed callback delay. */
    FIXED_CALLBACK_DELAY;
  }
}
