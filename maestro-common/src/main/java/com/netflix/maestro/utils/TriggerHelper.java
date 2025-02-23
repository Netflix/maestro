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

import com.cronutils.mapper.CronMapper;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.models.trigger.PredefinedTimeTrigger;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.models.trigger.TimeTriggerWithJitter;
import java.text.ParseException;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;

/** Cron Helper utility class. */
@Slf4j
public final class TriggerHelper {
  /** Private constructor for utility class. */
  private TriggerHelper() {}

  /**
   * Build cron from expression.
   *
   * @param cron cron string
   * @return cron expression object
   * @throws ParseException parse error
   */
  public static CronExpression buildCron(String cron) throws ParseException {
    return buildCron(cron, Defaults.DEFAULT_TIMEZONE);
  }

  /**
   * Build cron from expression and timezone string.
   *
   * @param cron cron string
   * @param timezone timezone
   * @return cron expression object
   * @throws ParseException parse error
   */
  public static CronExpression buildCron(String cron, String timezone) throws ParseException {
    return buildCron(cron, DateTimeZone.forID(timezone).toTimeZone());
  }

  /**
   * Build cron from cron expression and timezone.
   *
   * @param cron cron string
   * @param timezone timezone
   * @return the cron expression object
   * @throws ParseException parse error
   */
  public static CronExpression buildCron(String cron, TimeZone timezone) throws ParseException {
    String cronStr = cron;
    CronParser unixParser =
        new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    try {
      Cron parsedCron = unixParser.parse(cron);
      cronStr = CronMapper.fromUnixToQuartz().map(parsedCron).asString();
    } catch (IllegalArgumentException e) {
      LOG.trace("Unix cron parsing not successful for " + cron, e);
    }
    CronExpression cronExpression = new CronExpression(cronStr);
    cronExpression.setTimeZone(timezone);
    return cronExpression;
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
  @SneakyThrows
  public static Optional<Date> nextExecutionDate(
      TimeTrigger trigger, Date startDate, String uniqueId) {
    CronTimeTrigger cronTimeTrigger = getCronTimeTrigger(trigger);
    if (cronTimeTrigger != null) {
      CronExpression cronExpression =
          TriggerHelper.buildCron(cronTimeTrigger.getCron(), cronTimeTrigger.getTimezone());
      Date nextTime = cronExpression.getNextValidTimeAfter(startDate);
      if (nextTime != null) {
        nextTime.setTime(
            nextTime.getTime()
                + getDelayInSeconds(cronTimeTrigger, uniqueId) * TimeTrigger.MS_IN_SECONDS);
      }
      return Optional.ofNullable(nextTime);
    }

    throw new UnsupportedOperationException(
        "TimeTrigger nextExecutionDate is not implemented for type: " + trigger.getType());
  }

  private static CronTimeTrigger getCronTimeTrigger(TimeTrigger trigger) {
    CronTimeTrigger cronTimeTrigger = null;
    if (trigger instanceof CronTimeTrigger) {
      cronTimeTrigger = (CronTimeTrigger) trigger;
    } else if (trigger instanceof PredefinedTimeTrigger) {
      PredefinedTimeTrigger timeTrigger = (PredefinedTimeTrigger) trigger;
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
