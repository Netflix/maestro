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

import com.netflix.maestro.models.ValidationLimits;
import com.netflix.maestro.server.properties.MaestroProperties;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Bridges Spring-bound {@link MaestroProperties} into the Spring-free {@link ValidationLimits}
 * holder so that constraint validators in maestro-common can pick up the configured limits.
 */
@Configuration
@Slf4j
public class ValidationConfiguration {

  private final MaestroProperties maestroProperties;

  public ValidationConfiguration(MaestroProperties maestroProperties) {
    this.maestroProperties = maestroProperties;
  }

  @PostConstruct
  public void initValidationLimits() {
    int idLimit = maestroProperties.getValidation().getIdLengthLimit();
    int nameLimit = maestroProperties.getValidation().getNameLengthLimit();
    ValidationLimits.initialize(idLimit, nameLimit);
    LOG.info(
        "Initialized ValidationLimits: idLengthLimit={}, nameLengthLimit={}", idLimit, nameLimit);
  }
}
