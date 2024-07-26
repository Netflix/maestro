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
package com.netflix.maestro.server.validations;

import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.validations.AlertingValidator;
import jakarta.validation.ConstraintValidatorContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Noop implementation for testing purpose. */
@Component
@ConditionalOnProperty(value = "maestro.alerting.type", havingValue = "noop", matchIfMissing = true)
@Slf4j
public class AlertingNoopValidator implements AlertingValidator {

  @Override
  public boolean isValid(Alerting alerting, ConstraintValidatorContext context) {
    LOG.info("Skipping the alerting validation");
    return true;
  }
}
