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
package com.netflix.maestro.models.api;

/**
 * Upstream restart mode defines how callers want to restart for upstream. Only applicable to
 * restarts from a step.
 */
public enum UpstreamRestartMode {
  /**
   * If the step is within a foreach iteration, restart will happen in the ascendant non-inline
   * workflow instance (i.e. INLINE_ROOT) instead of the step itself. Also note that the restarted
   * step might not be the leaf-most step.
   *
   * <p>For example, if a workflow called [root.wf] has two levels of nested foreach loops, and
   * leaf-most step is restarted with this option, it would be equivalent to restart [root.wf]
   * instance with step parameter override for all iterations.
   *
   * <p>If the step is not within a foreach iteration, restart will happen in its own workflow
   * instance instead of the step itself.
   *
   * <p>For example, if a non-foreach workflow called [root.wf] has a step, which is restarted with
   * this option, it would be equivalent to restart [root.wf] instance with step parameter override
   * for all iterations.
   */
  RESTART_FROM_INLINE_ROOT,
  /**
   * It is equivalent of not providing any upstream restart mode, just restart from a current step.
   *
   * <p>For example, if a workflow called [root.wf] has a step, which is restarted with this option,
   * this step will be restarted and all upstream ascendant entities will be restarted using
   * RESTART_FROM_SPECIFIC if applicable.
   */
  RESTART_FROM_STEP;
}
