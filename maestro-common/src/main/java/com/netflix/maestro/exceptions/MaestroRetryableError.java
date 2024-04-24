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

/** Maestro backend retryable error. */
@Getter
public class MaestroRetryableError extends MaestroRuntimeException {
  private static final long serialVersionUID = 2554668492533395124L;

  private final Details details;

  /**
   * Constructor with an error message template and its arguments.
   *
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroRetryableError(String template, Object... args) {
    super(Code.INTERNAL_ERROR, String.format(template, args));
    this.details = Details.create(template, args);
  }

  /**
   * Constructor with an error message with details.
   *
   * @param details error details
   * @param msg error message
   */
  public MaestroRetryableError(Details details, String msg) {
    super(Code.INTERNAL_ERROR, msg, details.getCause());
    this.details = details;
  }

  /**
   * Constructor with cause and error message.
   *
   * @param cause cause exception
   */
  public MaestroRetryableError(Throwable cause, String msg) {
    super(Code.INTERNAL_ERROR, msg, cause);
    this.details = Details.create(cause, true, msg);
  }

  /**
   * Constructor with cause and error message and template.
   *
   * @param cause exception
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroRetryableError(Throwable cause, String template, Object... args) {
    this(cause, String.format(template, args));
  }
}
