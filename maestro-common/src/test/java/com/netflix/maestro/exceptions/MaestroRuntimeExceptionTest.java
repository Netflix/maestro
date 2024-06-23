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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import org.junit.Test;

public class MaestroRuntimeExceptionTest {
  @Test
  public void testCreateMaestroStatusCode() {
    for (MaestroRuntimeException.Code code : MaestroRuntimeException.Code.values()) {
      assertEquals(code, MaestroRuntimeException.Code.create(code.getStatusCode()));
    }
    AssertHelper.assertThrows(
        "Not a valid maestro status code",
        MaestroInvalidStatusException.class,
        "Invalid status code: 418",
        () -> MaestroRuntimeException.Code.create(418));
  }
}
