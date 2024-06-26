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

/** Maestro Workflow invalid expression exception. */
public class MaestroInvalidExpressionException extends MaestroRuntimeException {
  private static final long serialVersionUID = -5554668492523995523L;

  /**
   * Constructor with an error message template with arguments.
   *
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroInvalidExpressionException(String template, Object... args) {
    super(Code.BAD_REQUEST, String.format(template, args));
  }

  /**
   * Constructor with throwable and an error message template with arguments.
   *
   * @param cause throwable error
   * @param template template string following String.format()
   * @param args arguments
   */
  public MaestroInvalidExpressionException(Throwable cause, String template, Object... args) {
    super(Code.BAD_REQUEST, String.format(template, args), cause);
  }
}
