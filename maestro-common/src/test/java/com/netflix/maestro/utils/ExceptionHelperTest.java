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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ExceptionHelperTest {

  @Test
  public void testExceptionStackTrace() {
    try {
      throw new Exception("error happened!!");
    } catch (Exception e) {
      assertEquals(0, ExceptionHelper.getStackTrace(e, 0).length());
      assertEquals("java.lang.Exception: error happened!!", ExceptionHelper.getStackTrace(e, 1));
      assertTrue(ExceptionHelper.getStackTrace(e, 2000).length() > 100);
    }
  }
}
