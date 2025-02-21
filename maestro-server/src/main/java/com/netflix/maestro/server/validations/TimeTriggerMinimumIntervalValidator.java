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
package com.netflix.maestro.server.validations;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.utils.TriggerHelper;
import com.netflix.maestro.validations.TimeTriggerValidator;
import java.util.Date;
import java.util.Optional;
import javax.validation.ConstraintValidatorContext;
import org.springframework.stereotype.Component;

/**
 * TimeTrigger validation.
 *
 * <p>This ensures that the interval between triggers is not too small (>= `MINIMUM_INTERVAL`). Very
 * short intervals might hurt the system's performance.
 */
@Component
public class TimeTriggerMinimumIntervalValidator implements TimeTriggerValidator {
  @Override
  public boolean isValid(TimeTrigger trigger, ConstraintValidatorContext context) {
    Optional<Date> d1 = TriggerHelper.nextExecutionDate(trigger, new Date(), "");
    if (d1.isEmpty()) {
      return true;
    }
    Optional<Date> d2 = TriggerHelper.nextExecutionDate(trigger, d1.get(), "");
    if (d2.isEmpty()) {
      return true;
    }

    long period = d2.get().getTime() - d1.get().getTime();

    if (period < Constants.TIME_TRIGGER_MINIMUM_INTERVAL) {
      context
          .buildConstraintViolationWithTemplate(
              String.format(
                  "[time-trigger] the interval between time triggers is less than the minimal value [%s] millis",
                  Constants.TIME_TRIGGER_MINIMUM_INTERVAL))
          .addConstraintViolation();
      return false;
    }
    return true;
  }
}
