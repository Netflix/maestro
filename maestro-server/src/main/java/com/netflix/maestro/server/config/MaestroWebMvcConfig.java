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
package com.netflix.maestro.server.config;

import com.netflix.maestro.utils.JsonHelper;
import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Maestro MVC configuration for custom message converters. */
@Configuration
public class MaestroWebMvcConfig implements WebMvcConfigurer {

  // register maestro yaml converter
  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    MappingJackson2HttpMessageConverter yamlConverter =
        new MappingJackson2HttpMessageConverter(JsonHelper.objectMapperWithYaml());
    yamlConverter.setSupportedMediaTypes(List.of(MediaType.APPLICATION_YAML));
    // addFirst to prioritize maestro YAML object mapper
    converters.addFirst(yamlConverter);
  }
}
