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
package com.netflix.maestro.server.config;

import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.server.interceptor.UserInfoInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Maestro interceptor configuration for getting user info. */
@Configuration
public class MaestroInterceptorMvcConfig implements WebMvcConfigurer {
  private final UserInfoInterceptor userInfoInterceptor;

  MaestroInterceptorMvcConfig(UserInfoInterceptor userInfoInterceptor) {
    this.userInfoInterceptor = userInfoInterceptor;
  }

  /** register customized interceptor. */
  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry
        .addInterceptor(userInfoInterceptor)
        .addPathPatterns(
            "/api/v3/workflows/**",
            "/api/v3/groups/**",
            "/api/v3/tag-permits/**",
            "/api/v3/job-templates/**");
  }

  /** create request scope bean for caller. */
  @Bean
  @Scope(value = WebApplicationContext.SCOPE_REQUEST, proxyMode = ScopedProxyMode.TARGET_CLASS)
  public User.UserBuilder callerBuilder() {
    return User.builder();
  }
}
