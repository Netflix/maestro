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
package com.netflix.maestro.server.interceptor;

import com.netflix.maestro.models.definition.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.HandlerInterceptor;

/** A http request interceptor to get the user info from the header. */
@Slf4j
public class UserInfoInterceptor implements HandlerInterceptor {
  private final User.UserBuilder callerBuilder;

  /** constructor. */
  public UserInfoInterceptor(User.UserBuilder callerBuilder) {
    this.callerBuilder = callerBuilder;
  }

  /** customized pre handle method. */
  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    String userName = request.getHeader("user");
    callerBuilder.name(userName);
    LOG.info(
        "Received a HTTP {} request to uri {} from caller [{}] ",
        request.getMethod(),
        request.getRequestURI(),
        userName);
    return true;
  }
}
