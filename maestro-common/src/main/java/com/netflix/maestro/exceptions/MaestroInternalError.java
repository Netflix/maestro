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

/** Maestro backend non-retryable error. */
@Getter
public class MaestroInternalError extends MaestroRuntimeException {
  private static final long serialVersionUID = 2554668492533395123L;

  private final Details details;

  /**
   * Constructor with an error message template and its arguments.
   *
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroInternalError(String template, Object... args) {
    super(Code.INTERNAL_ERROR, String.format(template, args));
    this.details = Details.create(template, args);
  }

  /**
   * Constructor with an error message with details.
   *
   * @param details error details
   * @param msg error message
   */
  public MaestroInternalError(Details details, String msg) {
    super(Code.INTERNAL_ERROR, msg);
    this.details = details;
  }

  /**
   * Constructor with error message and details.
   *
   * @param cause cause exception
   */
  public MaestroInternalError(Throwable cause, String msg) {
    super(Code.INTERNAL_ERROR, msg, cause);
    this.details = Details.create(cause, false, msg);
  }

  /** Constructor with Throwable and template. */
  public MaestroInternalError(Throwable cause, String template, Object... args) {
    super(String.format(template, args), cause);
    this.details = Details.create(cause, false, String.format(template, args));
  }
}
