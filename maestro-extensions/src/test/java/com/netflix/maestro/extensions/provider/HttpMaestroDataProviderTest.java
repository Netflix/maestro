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
package com.netflix.maestro.extensions.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class HttpMaestroDataProviderTest extends ExtensionsBaseTest {
  private static final String BASE_URL = "http://localhost:8080/api/v3";

  @Mock private HttpClient httpClient;
  @Mock private HttpResponse<String> httpResponse;

  private ObjectMapper objectMapper;
  private HttpMaestroDataProvider provider;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    objectMapper = MAPPER;
    provider = new HttpMaestroDataProvider(BASE_URL, objectMapper, httpClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetWorkflowInstance() throws Exception {
    var instance = new WorkflowInstance();
    instance.setWorkflowId("test-wf");
    instance.setWorkflowInstanceId(1L);
    instance.setWorkflowRunId(1L);
    String json = objectMapper.writeValueAsString(instance);

    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(json);
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(httpResponse);

    WorkflowInstance result = provider.getWorkflowInstance("test-wf", 1L, 1L);
    assertThat(result.getWorkflowId()).isEqualTo("test-wf");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetWorkflowDefinition() throws Exception {
    var definition = new WorkflowDefinition();
    String json = objectMapper.writeValueAsString(definition);

    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(json);
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(httpResponse);

    WorkflowDefinition result = provider.getWorkflowDefinition("test-wf", "1");
    assertThat(result).isNotNull();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNotFoundThrowsMaestroNotFoundException() throws Exception {
    when(httpResponse.statusCode()).thenReturn(404);
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(httpResponse);

    assertThatThrownBy(() -> provider.getWorkflowInstance("test-wf", 1L, 1L))
        .isInstanceOf(MaestroNotFoundException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testServerErrorThrowsMaestroRuntimeException() throws Exception {
    when(httpResponse.statusCode()).thenReturn(500);
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(httpResponse);

    assertThatThrownBy(() -> provider.getWorkflowInstance("test-wf", 1L, 1L))
        .isInstanceOf(MaestroRuntimeException.class)
        .hasMessageContaining("failed with status 500");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIOExceptionThrowsMaestroRuntimeException() throws Exception {
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenThrow(new IOException("connection refused"));

    assertThatThrownBy(() -> provider.getWorkflowInstance("test-wf", 1L, 1L))
        .isInstanceOf(MaestroRuntimeException.class)
        .hasMessageContaining("connection refused");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetStepInstance() throws Exception {
    var stepInstance = new StepInstance();
    String json = objectMapper.writeValueAsString(stepInstance);

    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(json);
    when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(httpResponse);

    StepInstance result = provider.getStepInstance("test-wf", 1L, 1L, "step1", 1L);
    assertThat(result).isNotNull();
  }
}
