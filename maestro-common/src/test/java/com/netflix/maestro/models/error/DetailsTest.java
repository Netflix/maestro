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
package com.netflix.maestro.models.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class DetailsTest extends MaestroBaseTest {

  @Test
  public void testCreateWithStackTrace() {
    Exception exception = new Exception(new Exception(new Exception("test")));
    Details details = Details.create(exception, false, "test-msg");
    assertEquals(MaestroRuntimeException.Code.INTERNAL_ERROR, details.getStatus());
    assertEquals("test-msg", details.getMessage());
    assertEquals(6, details.getErrors().size());
    assertFalse(details.isRetryable());

    exception.setStackTrace(new StackTraceElement[0]);
    details = Details.create(exception, false, "test-msg");
    assertEquals(MaestroRuntimeException.Code.INTERNAL_ERROR, details.getStatus());
    assertEquals("test-msg", details.getMessage());
    assertEquals(3, details.getErrors().size());
    assertFalse(details.isRetryable());
  }

  @Test
  public void testCreateWithoutStackTrace() {
    Exception exception = new Exception(new Exception(new Exception("test")));
    Details details = Details.create(exception, true, "test-msg");
    assertEquals(MaestroRuntimeException.Code.INTERNAL_ERROR, details.getStatus());
    assertEquals("test-msg", details.getMessage());
    assertEquals(3, details.getErrors().size());
    assertTrue(details.isRetryable());
  }

  @Test
  public void testCreateCausedByMaestroInternalError() {
    Exception exception = new MaestroInternalError("test");
    Details details = Details.create(exception, true, "test-msg");
    assertEquals(MaestroRuntimeException.Code.INTERNAL_ERROR, details.getStatus());
    assertEquals("test-msg", details.getMessage());
    assertEquals(1, details.getErrors().size());
    assertEquals(Collections.singletonList("MaestroInternalError: test"), details.getErrors());
    assertTrue(details.isRetryable());
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    Details details = loadObject("fixtures/errors/sample-details.json", Details.class);
    Details deserializedDetails =
        MAPPER.readValue(MAPPER.writeValueAsString(details), Details.class);

    String ser1 = MAPPER.writeValueAsString(deserializedDetails);
    Details actual =
        MAPPER.readValue(MAPPER.writeValueAsString(deserializedDetails), Details.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(details.getCause().toString(), deserializedDetails.getCause().toString());
    Assertions.assertThat(details)
        .usingRecursiveComparison()
        .ignoringFields("cause")
        .isEqualTo(deserializedDetails);
    assertEquals(ser1, ser2);
  }
}
