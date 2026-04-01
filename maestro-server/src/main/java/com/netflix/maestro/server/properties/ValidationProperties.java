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
package com.netflix.maestro.server.properties;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.utils.ValidationLimits;
import lombok.Getter;
import lombok.Setter;

/**
 * Configurable validation limit properties. Bound under {@code maestro.validation.*}. Defaults
 * match the compile-time constants in {@link Constants} so existing deployments are unaffected
 * unless the values are explicitly overridden.
 */
@Getter
@Setter
public class ValidationProperties implements ValidationLimits {
  /** Maximum allowed length for Maestro IDs (workflow id, step id, param names). Default: 128. */
  private int idLengthLimit = Constants.ID_LENGTH_LIMIT;

  /** Maximum allowed length for Maestro names (workflow name, step name). Default: 256. */
  private int nameLengthLimit = Constants.NAME_LENGTH_LIMIT;
}
