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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.dsl.DslWorkflowDef;
import com.netflix.maestro.dsl.parsers.WorkflowParser;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.yaml.MappingJackson2YamlHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Maestro MVC configuration for custom message converters. */
@Configuration
public class MaestroWebMvcConfig implements WebMvcConfigurer {

  private final WorkflowParser workflowParser;
  private final ObjectMapper yamlMapper;

  public MaestroWebMvcConfig(
      WorkflowParser workflowParser, @Qualifier("ObjectMapperWithYaml") ObjectMapper yamlMapper) {
    this.workflowParser = workflowParser;
    this.yamlMapper = yamlMapper;
  }

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    MappingJackson2YamlHttpMessageConverter yamlConverter =
        new MappingJackson2YamlHttpMessageConverter(yamlMapper) {
          @Override
          public boolean canWrite(Class<?> clazz, MediaType mediaType) {
            return false; // disable writing YAML responses
          }
        };
    converters.addFirst(yamlConverter);
    converters.addFirst(new DslToWorkflowCreateRequestConverter(workflowParser, yamlMapper));
  }

  /** Custom message converter that converts YAML DslWorkflowDef to WorkflowCreateRequest. */
  public static class DslToWorkflowCreateRequestConverter
      extends AbstractHttpMessageConverter<WorkflowCreateRequest> {

    private final WorkflowParser parser;
    private final ObjectMapper yamlMapper;

    /** Constructor. */
    public DslToWorkflowCreateRequestConverter(WorkflowParser parser, ObjectMapper yamlMapper) {
      super(MediaType.APPLICATION_YAML);
      this.parser = parser;
      this.yamlMapper = yamlMapper;
    }

    @Override
    protected boolean supports(Class<?> clazz) {
      return WorkflowCreateRequest.class.equals(clazz);
    }

    @Override
    protected WorkflowCreateRequest readInternal(
        Class<? extends WorkflowCreateRequest> clazz, HttpInputMessage inputMessage)
        throws IOException, HttpMessageNotReadableException {
      DslWorkflowDef dslWorkflowDef =
          yamlMapper.readValue(inputMessage.getBody(), DslWorkflowDef.class);
      return parser.toWorkflowCreateRequest(dslWorkflowDef);
    }

    @Override
    protected void writeInternal(
        WorkflowCreateRequest workflowCreateRequest, HttpOutputMessage outputMessage)
        throws IOException, HttpMessageNotWritableException {
      throw new UnsupportedOperationException(
          "Writing WorkflowCreateRequest as YAML is not supported");
    }
  }
}
