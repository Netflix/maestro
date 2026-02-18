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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link MaestroDataProvider} that makes HTTP calls to maestro-server REST API.
 * This mirrors internal Maestro's MaestroClient which calls maestro-server over HTTP.
 */
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class HttpMaestroDataProvider implements MaestroDataProvider {
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_OK = 200;
  private static final String GET_WORKFLOW_INSTANCE_URL = "/workflows/%s/instances/%s/runs/%s";
  private static final String GET_WORKFLOW_DEFINITION_URL = "/workflows/%s/versions/%s";
  private static final String GET_STEP_INSTANCE_URL =
      "/workflows/%s/instances/%s/runs/%s/steps/%s/attempts/%s";

  private final String baseUrl;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;

  public HttpMaestroDataProvider(String baseUrl, ObjectMapper objectMapper, HttpClient httpClient) {
    this.baseUrl = baseUrl;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
  }

  @Override
  public WorkflowInstance getWorkflowInstance(String workflowId, long instanceId, long runId) {
    String path = String.format(GET_WORKFLOW_INSTANCE_URL, workflowId, instanceId, runId);
    return sendGetRequest(path, WorkflowInstance.class);
  }

  @Override
  public WorkflowDefinition getWorkflowDefinition(String workflowId, String version) {
    String path = String.format(GET_WORKFLOW_DEFINITION_URL, workflowId, version);
    return sendGetRequest(path, WorkflowDefinition.class);
  }

  @Override
  public StepInstance getStepInstance(
      String workflowId, long instanceId, long runId, String stepId, long attemptId) {
    String path =
        String.format(GET_STEP_INSTANCE_URL, workflowId, instanceId, runId, stepId, attemptId);
    return sendGetRequest(path, StepInstance.class);
  }

  private <T> T sendGetRequest(String path, Class<T> clazz) {
    String url = baseUrl + path;
    LOG.debug("Sending GET request to {}", url);
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("Accept", "application/json")
              .GET()
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      int statusCode = response.statusCode();
      if (statusCode == HTTP_NOT_FOUND) {
        throw new MaestroNotFoundException("Resource not found at %s", url);
      }
      if (statusCode != HTTP_OK) {
        throw new MaestroRuntimeException(
            MaestroRuntimeException.Code.INTERNAL_ERROR,
            String.format("HTTP request to %s failed with status %s", url, statusCode));
      }
      return objectMapper.readValue(response.body(), clazz);
    } catch (MaestroNotFoundException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MaestroRuntimeException(
          MaestroRuntimeException.Code.INTERNAL_ERROR,
          String.format("HTTP request to %s was interrupted", url),
          e);
    } catch (IOException e) {
      throw new MaestroRuntimeException(
          MaestroRuntimeException.Code.INTERNAL_ERROR,
          String.format("HTTP request to %s failed: %s", url, e.getMessage()),
          e);
    }
  }
}
