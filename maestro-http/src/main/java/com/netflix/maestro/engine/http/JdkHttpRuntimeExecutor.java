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

import com.netflix.maestro.engine.properties.HttpStepProperties;
import com.netflix.maestro.models.stepruntime.HttpStepRequest;
import com.netflix.maestro.utils.ObjectHelper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of HttpRuntimeExecutor using JDK's built-in java.net.http.HttpClient introduced
 * since Java 11.
 */
@Slf4j
public class JdkHttpRuntimeExecutor implements HttpRuntimeExecutor {
  private final HttpClient httpClient;
  private final Duration sendTimeout;
  private final UrlValidator urlValidator;

  /**
   * Constructor.
   *
   * @param httpClient the HttpClient to use
   * @param properties the HttpStepProperties to use
   * @param urlValidator the UrlValidator for validating URLs
   */
  public JdkHttpRuntimeExecutor(
      HttpClient httpClient, HttpStepProperties properties, UrlValidator urlValidator) {
    this.httpClient = httpClient;
    this.sendTimeout = Duration.ofMillis(properties.getSendTimeout());
    this.urlValidator = urlValidator;
  }

  @Override
  public HttpResponse<String> execute(HttpStepRequest request)
      throws IOException, InterruptedException {
    URI uri = urlValidator.validateAndParseUri(request.getUrl());
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().uri(uri).timeout(sendTimeout);
    if (request.getHeaders() != null) {
      request.getHeaders().forEach(requestBuilder::header);
    }
    HttpRequest.BodyPublisher bodyPublisher =
        ObjectHelper.isNullOrEmpty(request.getBody())
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(request.getBody());
    requestBuilder.method(request.getMethod(), bodyPublisher);

    return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
  }
}
