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
package com.netflix.maestro.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/** Utility for Exceptions. */
public final class ExceptionHelper {

  private static final String NEWLINE = "\n";

  private ExceptionHelper() {}

  /**
   * Get stackTrace with maximum number of lines returned.
   *
   * @param throwable throwable error
   * @param maxLines max number
   * @return bounded stacktrace string
   */
  public static String getStackTrace(Throwable throwable, long maxLines) {
    StringWriter writer = new StringWriter();
    PrintWriter pw = new PrintWriter(writer, true);
    throwable.printStackTrace(pw);
    String[] lines = writer.toString().split(NEWLINE);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < Math.min(lines.length, maxLines); i++) {
      if (i > 0) {
        sb.append(NEWLINE);
      }
      sb.append(lines[i]);
    }
    return sb.toString();
  }
}
