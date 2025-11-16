/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.http;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class SizeBoundedBodyHandlerTest extends MaestroBaseTest {
  @Mock private HttpResponse.ResponseInfo responseInfo;

  @Before
  public void setUp() {
    HttpHeaders headers =
        HttpHeaders.of(Map.of("Content-Length", List.of(String.valueOf(1024))), (k, v) -> true);
    when(responseInfo.headers()).thenReturn(headers);
  }

  @Test
  public void testThrowWhenExceedingBoundBasedOnContentLength() {
    SizeBoundedBodyHandler handler = new SizeBoundedBodyHandler(100);
    AssertHelper.assertThrows(
        "Response exceeds size limit",
        IllegalArgumentException.class,
        "Http response content length [1024] is not between 0 and 100",
        () -> handler.apply(responseInfo));
  }

  @Test
  public void testApplyWithContentLengthBelowLimit() {
    SizeBoundedBodyHandler handler = new SizeBoundedBodyHandler(2048);
    assertNotNull(handler.apply(responseInfo));
  }

  @Test
  public void testAcceptsResponseWithoutContentLength() {
    HttpHeaders headers = HttpHeaders.of(Map.of(), (k, v) -> true);
    when(responseInfo.headers()).thenReturn(headers);

    SizeBoundedBodyHandler handler = new SizeBoundedBodyHandler(1024);
    assertNotNull(handler.apply(responseInfo));
  }

  @Test
  public void testInvalidMaxResponseSize() {
    AssertHelper.assertThrows(
        "Constructor validation",
        IllegalArgumentException.class,
        "Invalid maxResponseSize [0] as it is not a positive int number.",
        () -> new SizeBoundedBodyHandler(0));

    AssertHelper.assertThrows(
        "Constructor validation",
        IllegalArgumentException.class,
        "Invalid maxResponseSize [-1] as it is not a positive int number.",
        () -> new SizeBoundedBodyHandler(-1));

    AssertHelper.assertThrows(
        "Constructor validation",
        IllegalArgumentException.class,
        "Invalid maxResponseSize [9223372036854775807] as it is not a positive int number.",
        () -> new SizeBoundedBodyHandler(Long.MAX_VALUE));
  }
}
