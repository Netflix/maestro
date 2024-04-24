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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;

/** Maestro Workflow Invalid status exception. */
@Getter
public class MaestroInvalidStatusException extends MaestroRuntimeException {
  private static final long serialVersionUID = -5554778492523995523L;

  private final List<String> errors;

  /**
   * Constructor with error message and details.
   *
   * @param cause cause exception
   * @param msg exception message
   * @param errors detailed error messages
   */
  public MaestroInvalidStatusException(Throwable cause, String msg, String... errors) {
    super(Code.BAD_REQUEST, msg, cause);
    this.errors = Objects.isNull(errors) ? Collections.emptyList() : Arrays.asList(errors);
  }

  /**
   * Constructor with error details and an error message template with arguments.
   *
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroInvalidStatusException(String template, Object... args) {
    super(Code.BAD_REQUEST, String.format(template, args));
    this.errors = Collections.emptyList();
  }
}
