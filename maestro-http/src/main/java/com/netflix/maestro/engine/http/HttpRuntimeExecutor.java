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

import com.netflix.maestro.models.stepruntime.HttpStepRequest;
import java.io.IOException;
import java.net.http.HttpResponse;

/** Interface for HTTP runtime executor to execute HTTP requests. */
@FunctionalInterface
public interface HttpRuntimeExecutor {
  /**
   * Execute an HTTP request.
   *
   * @param request the HTTP request to execute
   * @return the HTTP response
   * @throws IOException – if an I/O error occurs when sending or receiving, or the client has
   *     closing shut down
   * @throws InterruptedException – if the operation is interrupted
   */
  HttpResponse<String> execute(HttpStepRequest request) throws IOException, InterruptedException;
}
