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

import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * A body handler that enforces response size limit to prevent OOM risk and DoS attacks.
 *
 * <p>todo: once upgrade to JDK 25, replace it with HttpResponse.BodyHandlers.limiting(...).
 */
public class SizeBoundedBodyHandler implements HttpResponse.BodyHandler<String> {
  private static final String CONTENT_LENGTH_HEADER = "Content-Length";

  private final int maxResponseSize;

  /**
   * Constructor.
   *
   * @param maxResponseSize the response size upper bound in bytes
   */
  public SizeBoundedBodyHandler(long maxResponseSize) {
    Checks.checkTrue(
        maxResponseSize <= Integer.MAX_VALUE && maxResponseSize > 0,
        "Invalid maxResponseSize [%s] as it is not a positive int number.",
        maxResponseSize);
    this.maxResponseSize = (int) maxResponseSize;
  }

  @Override
  public HttpResponse.BodySubscriber<String> apply(HttpResponse.ResponseInfo responseInfo) {
    responseInfo
        .headers()
        .firstValueAsLong(CONTENT_LENGTH_HEADER)
        .ifPresent(
            contentLength -> {
              Checks.checkTrue(
                  contentLength >= 0 && contentLength <= maxResponseSize,
                  "Http response content length [%s] is not between 0 and %s",
                  contentLength,
                  maxResponseSize);
            });

    return HttpResponse.BodySubscribers.mapping(
        HttpResponse.BodySubscribers.ofInputStream(),
        inputStream -> {
          try (inputStream) {
            // Read maxResponseSize + 1 bytes to detect if the upper bound is exceeded
            byte[] bytes = inputStream.readNBytes(maxResponseSize + 1);
            Checks.checkTrue(
                bytes.length <= maxResponseSize,
                "Http response size exceeds the size limit [%s] bytes",
                maxResponseSize);
            return new String(bytes, StandardCharsets.UTF_8); // assuming UTF-8 encoding
          } catch (IOException e) {
            throw new MaestroRuntimeException("Failed to read response body", e);
          }
        });
  }
}
