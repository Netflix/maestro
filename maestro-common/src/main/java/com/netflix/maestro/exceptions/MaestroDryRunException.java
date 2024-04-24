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
package com.netflix.maestro.exceptions;

import com.netflix.maestro.models.error.Details;
import lombok.Getter;

/** Maestro Validation exception. */
@Getter
public class MaestroDryRunException extends MaestroRuntimeException {
  private static final long serialVersionUID = -597334728023305632L;

  private final Details details;

  /** Constructor. */
  public MaestroDryRunException(String template, Object... args) {
    super(Code.BAD_REQUEST, String.format(template, args));
    this.details = Details.create(template, args);
  }

  /** Constructor. */
  public MaestroDryRunException(Throwable cause, String template, Object... args) {
    super(Code.BAD_REQUEST, String.format(template, args), cause);
    this.details = Details.create(cause, false, String.format(template, args));
  }
}
