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
package com.netflix.maestro.utils;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.models.trigger.PredefinedTimeTrigger;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.models.trigger.TimeTriggerWithJitter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/** Cron Helper utility class. */
@Slf4j
public final class TriggerHelper {
  /** Private constructor for utility class. */
  private TriggerHelper() {}

  private record ParsedCron(ExecutionTime executionTime, ZoneId zoneId) {}

  /**
   * Build cron from expression for validation.
   *
   * @param cron cron string
   * @throws IllegalArgumentException parse error
   */
  public static void validateCron(String cron) {
    buildCron(cron, Defaults.DEFAULT_TIMEZONE);
  }

  /**
   * Build cron from expression and timezone string.
   *
   * @param cron cron string
   * @param timezone timezone
   * @return parsed cron expression object with execution time
   * @throws IllegalArgumentException parse error
   */
  private static ParsedCron buildCron(String cron, String timezone) {
    return buildCron(
        cron,
        ObjectHelper.isNullOrEmpty(timezone) ? Defaults.DEFAULT_TIMEZONE : ZoneId.of(timezone));
  }

  /**
   * Build cron from cron expression and timezone id.
   *
   * @param cron cron string
   * @param zoneId time zone id
   * @return parsed cron expression object with execution time
   * @throws IllegalArgumentException parse error
   */
  private static ParsedCron buildCron(String cron, ZoneId zoneId) {
    CronParser unixParser =
        new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    Cron parsedCron;
    try {
      parsedCron = unixParser.parse(cron);
    } catch (IllegalArgumentException e) {
      LOG.trace("Unix cron parsing not successful for {}, trying Quartz format", cron, e);
      CronParser quartzParser =
          new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
      parsedCron = quartzParser.parse(cron);
    }
    return new ParsedCron(ExecutionTime.forCron(parsedCron), zoneId);
  }

  /**
   * Calculate next execution date after start date for a given time trigger.
   *
   * @param trigger time trigger object
   * @param startDate start date
   * @param uniqueId used to calculate fuzzy cron delay if enabled. This must be unique to a
   *     workflow, and ensuring uniqueness is the caller's responsibility.
   * @return next execution date if present
   */
  public static Optional<Date> nextExecutionDate(
      TimeTrigger trigger, Date startDate, String uniqueId) {
    CronTimeTrigger cronTimeTrigger = getCronTimeTrigger(trigger);
    if (cronTimeTrigger != null) {
      ParsedCron parsedCron =
          TriggerHelper.buildCron(cronTimeTrigger.getCron(), cronTimeTrigger.getTimezone());

      ZonedDateTime startDateTime =
          ZonedDateTime.ofInstant(startDate.toInstant(), parsedCron.zoneId());

      Optional<ZonedDateTime> nextExecution =
          parsedCron.executionTime().nextExecution(startDateTime);
      return nextExecution.map(
          next -> {
            Date nextTime = Date.from(next.toInstant());
            nextTime.setTime(
                nextTime.getTime()
                    + getDelayInSeconds(cronTimeTrigger, uniqueId) * TimeTrigger.MS_IN_SECONDS);
            return nextTime;
          });
    }

    throw new UnsupportedOperationException(
        "TimeTrigger nextExecutionDate is not implemented for type: " + trigger.getType());
  }

  private static CronTimeTrigger getCronTimeTrigger(TimeTrigger trigger) {
    CronTimeTrigger cronTimeTrigger = null;
    if (trigger instanceof CronTimeTrigger) {
      cronTimeTrigger = (CronTimeTrigger) trigger;
    } else if (trigger instanceof PredefinedTimeTrigger timeTrigger) {
      cronTimeTrigger = new CronTimeTrigger();
      cronTimeTrigger.setCron(timeTrigger.getExpression().key());
      cronTimeTrigger.setTimezone(timeTrigger.getTimezone());
      cronTimeTrigger.setFuzzyMaxDelay(timeTrigger.getFuzzyMaxDelay());
    }
    return cronTimeTrigger;
  }

  /**
   * Returns deterministic random jitter in seconds if enabled for a given TimeTriggerWithJitter
   * object.
   *
   * @param salt used to differentiate b/w same triggers across different workflows
   * @return delay in secs
   */
  private static int getDelayInSeconds(TimeTriggerWithJitter trigger, String salt) {
    if (trigger.getFuzzyMaxDelay() == null) {
      return 0;
    }
    long delayInMillis = DurationParser.getDurationInMillis(trigger.getFuzzyMaxDelay());
    if (delayInMillis == 0) {
      return 0;
    }
    UUID triggerUUID = IdHelper.createUuid(trigger + salt);
    Random rng = new Random(triggerUUID.getLeastSignificantBits());
    return rng.nextInt((int) (delayInMillis / TimeTrigger.MS_IN_SECONDS));
  }
}
