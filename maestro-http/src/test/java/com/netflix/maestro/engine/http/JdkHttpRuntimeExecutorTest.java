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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.properties.HttpStepProperties;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.stepruntime.HttpStepRequest;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class JdkHttpRuntimeExecutorTest extends MaestroBaseTest {
  @Mock private HttpClient httpClient;
  @Mock private SizeBoundedBodyHandler bodyHandler;

  private JdkHttpRuntimeExecutor executor;

  @Before
  public void setUp() {
    HttpStepProperties properties = new HttpStepProperties();
    properties.setSendTimeout(60000L);
    properties.setAllowList(Set.of("test.example"));
    UrlValidator urlValidator = new UrlValidator(properties);
    executor = new JdkHttpRuntimeExecutor(httpClient, properties, urlValidator, bodyHandler);
  }

  @Test
  public void testExecuteRequestWithoutBody() throws Exception {
    HttpStepRequest request =
        HttpStepRequest.builder()
            .url("https://test.example/api/test")
            .method("GET")
            .headers(Map.of("Accept", "application/json"))
            .build();

    executor.execute(request);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(requestCaptor.capture(), any());

    HttpRequest capturedRequest = requestCaptor.getValue();
    assertEquals("GET", capturedRequest.method());
    assertEquals("https://test.example/api/test", capturedRequest.uri().toString());
    assertEquals(0, capturedRequest.bodyPublisher().get().contentLength());
  }

  @Test
  public void testExecuteRequestWithBody() throws Exception {
    HttpStepRequest request =
        HttpStepRequest.builder()
            .url("https://test.example/api/test")
            .method("POST")
            .headers(Map.of("Content-Type", "application/json"))
            .body("{\"foo\":\"bar\"}")
            .build();

    executor.execute(request);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(requestCaptor.capture(), any());

    HttpRequest capturedRequest = requestCaptor.getValue();
    assertEquals("POST", capturedRequest.method());
    assertEquals("https://test.example/api/test", capturedRequest.uri().toString());
    assertEquals(13, capturedRequest.bodyPublisher().get().contentLength());
  }

  @Test
  public void testExecuteRejectsHostNotInAllowlist() {
    AssertHelper.assertThrows(
        "Invalid host",
        MaestroValidationException.class,
        "URL host [not.allowed.com] is not allowed. Please contact the administrator.",
        () ->
            executor.execute(
                HttpStepRequest.builder()
                    .url("http://not.allowed.com/admin")
                    .method("GET")
                    .build()));
  }

  @Test
  public void testExecuteRejectsInvalidScheme() {
    AssertHelper.assertThrows(
        "Invalid scheme",
        MaestroValidationException.class,
        "URL scheme [file] is not allowed.",
        () ->
            executor.execute(
                HttpStepRequest.builder().url("file:///etc/passwd").method("GET").build()));
  }
}
