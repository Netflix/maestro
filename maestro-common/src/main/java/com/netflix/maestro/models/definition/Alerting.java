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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.function.Function;

/**
 * Pluggable alerting config carried on a workflow's {@link Properties}. The engine does not
 * interpret the contents of this block; downstream consumers (notification services, dashboards,
 * runtime hooks) read it via their own logic.
 *
 * <p>The default implementation, {@link DefaultAlerting}, preserves the original Netflix-flavored
 * shape and is registered as the default abstract-type mapping inside {@link
 * com.netflix.maestro.utils.JsonHelper#objectMapper()}. Downstream consumers can replace it by
 * registering their own mapping:
 *
 * <pre>
 *   SimpleModule module = new SimpleModule();
 *   module.addAbstractTypeMapping(Alerting.class, MyAlerting.class);
 *   mapper.registerModule(module);
 * </pre>
 *
 * <p>A later-registered abstract-type mapping replaces the OSS default, so the consumer's impl
 * wins.
 *
 * <p>Each running JVM uses exactly one {@code Alerting} implementation, so no JSON type
 * discriminator is needed. Existing serialized data continues to deserialize as {@link
 * DefaultAlerting} on any mapper that doesn't register a different mapping.
 */
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface Alerting {
  /**
   * Update fields by parsing parameters within them. Implementations should resolve any parameter
   * references (typically {@code ${...}} placeholders) using the provided parser. Called by the
   * engine during workflow start.
   */
  @JsonIgnore
  void update(Function<ParamDefinition, Parameter> paramParser);
}
