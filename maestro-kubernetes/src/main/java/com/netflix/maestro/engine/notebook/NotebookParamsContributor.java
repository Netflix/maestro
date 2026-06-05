/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.engine.notebook;

import com.netflix.maestro.engine.execution.WorkflowSummary;
import java.util.Map;

/**
 * SPI that lets consumers contribute extra entries to the notebook param map alongside the standard
 * parameters built by {@link NotebookParamsBuilder}.
 *
 * <p>{@link NotebookParamsBuilder} invokes every registered contributor. The invocation order
 * follows the injected {@code List} order, which Spring does not strongly guarantee, so
 * contributors should write disjoint keys; if two contributors write the same key the result
 * depends on ordering and is effectively undefined. A contributor may noop when the workflow's
 * properties don't carry the shape it understands (typical for alerting-driven contributors that
 * only handle one concrete {@link com.netflix.maestro.models.definition.Alerting} implementation).
 *
 * <p>Contributors own their own dependencies (e.g., an {@code ObjectMapper} for nested JSON
 * serialization); the SPI itself stays narrow on purpose.
 */
@FunctionalInterface
public interface NotebookParamsContributor {
  /**
   * Contribute zero or more entries to the notebook param map.
   *
   * @param summary the workflow summary providing properties, params, and identity
   * @param paramMap mutable map that will be serialized as the notebook param payload
   */
  void contribute(WorkflowSummary summary, Map<String, Object> paramMap);
}
