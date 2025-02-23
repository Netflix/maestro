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
package com.netflix.maestro.timetrigger.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class TimeTriggerExecutionPlannerTest extends MaestroBaseTest {
  private static final int MAX_EXECUTIONS = 20;
  public static final String US_PACIFIC = "US/Pacific";

  private TimeTriggerExecutionPlanner executionPlanner;

  @Before
  public void setUp() {
    executionPlanner = new TimeTriggerExecutionPlanner(MAX_EXECUTIONS);
  }

  private List<TimeTriggerWithWatermark> generateTriggers(
      String cronExpression, String triggerTs, String timezone, ParsableLong maxDelay) {
    return List.of(generateTrigger(cronExpression, triggerTs, timezone, maxDelay));
  }

  private TimeTriggerWithWatermark generateTrigger(
      String cronExpression, String triggerTs, String timezone, ParsableLong maxDelay) {
    CronTimeTrigger trigger = new CronTimeTrigger();
    trigger.setCron(cronExpression);
    trigger.setTimezone(timezone);
    if (maxDelay != null) {
      trigger.setFuzzyMaxDelay(maxDelay);
    }
    return TimeTriggerWithWatermark.builder()
        .timeTrigger(trigger)
        .lastTriggerTimestamp(ZonedDateTime.parse(triggerTs).toInstant().toEpochMilli())
        .build();
  }

  private String strFormat(Date date) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
        .withZone(TimeZone.getTimeZone(US_PACIFIC).toZoneId())
        .format(date.toInstant());
  }

  private Date parseDate(String date) {
    return Date.from(ZonedDateTime.parse(date).toInstant());
  }

  private Date parseDate(List<TimeTriggerWithWatermark> triggers, Duration plusAmount) {
    return Date.from(
        ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(triggers.getFirst().getLastTriggerTimestamp()),
                ZoneId.of(US_PACIFIC))
            .plus(plusAmount)
            .toInstant());
  }

  /** Earliest execution date. */
  @Test
  public void testCalculateEarliestExecutionDate() {
    var triggers =
        generateTriggers("0 30 2 7 5 ? 2021", "2021-01-01T00:00:00-00:00", US_PACIFIC, null);
    Optional<Date> earliestDate =
        executionPlanner.calculateEarliestExecutionDate(triggers, "test-workflow");
    assertEquals(parseDate("2021-05-07T02:30:00-07:00"), earliestDate.get());
  }

  /** Earliest execution date with fuzzy delay enabled. */
  @Test
  public void testCalculateEarliestExecutionDateWithFuzzy() {
    var triggers =
        generateTriggers(
            "0 30 2 7 5 ? 2021",
            "2021-01-01T00:00:00-00:00",
            US_PACIFIC,
            ParsableLong.of("5 mins"));
    Optional<Date> earliestDate =
        executionPlanner.calculateEarliestExecutionDate(triggers, "test-workflow");

    var dateTime = ZonedDateTime.parse("2021-05-07T02:30:00-07:00");
    Date minDate = Date.from(dateTime.toInstant());
    Date maxDate = Date.from(dateTime.plusMinutes(5).toInstant());

    assertFalse(minDate.after(earliestDate.get()));
    assertTrue(maxDate.after(earliestDate.get()));
  }

  /** Earliest execution date with multiple triggers. */
  @Test
  public void testCalculateEarliestExecutionDateMultiple() {
    var triggers =
        List.of(
            generateTrigger("0 30 2 7 12 ? 2021", "2022-01-01T00:00:00-00:00", US_PACIFIC, null),
            generateTrigger("5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null),
            generateTrigger("5,15 2,3 * * *", "2021-05-18T00:00:00-07:00", US_PACIFIC, null));
    Optional<Date> earliestDate =
        executionPlanner.calculateEarliestExecutionDate(triggers, "test-workflow");
    assertEquals(parseDate("2021-05-17T02:05:00-07"), earliestDate.get());
  }

  /** Earliest execution date with empty executions. */
  @Test
  public void testCalculateEarliestExecutionDateEmptyExecutions() {
    var triggers =
        generateTriggers("0 30 2 7 12 ? 2021", "2022-01-01T00:00:00-00:00", US_PACIFIC, null);
    Optional<Date> earliestDate =
        executionPlanner.calculateEarliestExecutionDate(triggers, "test-workflow");
    assertFalse(earliestDate.isPresent());
  }

  /** Empty planned executions. */
  @Test
  public void testCalculateEmptyPlannedExecutions() {
    var triggers = generateTriggers("5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null);

    Date endDate = parseDate(triggers, Duration.ofHours(1));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertTrue(planned.isEmpty());
  }

  /** Should limit max executions. */
  @Test
  public void testCalculateMaxExecutions() {
    var triggers = generateTriggers("*/1 * * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null);
    Date endDate = parseDate(triggers, Duration.ofDays(1));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(MAX_EXECUTIONS, planned.size());
  }

  /** Planned executions in PDT. */
  @Test
  public void testCalculatePlannedExecutionsPDT() {
    var triggers = generateTriggers("5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null);
    Date endDate = parseDate(triggers, Duration.ofDays(3));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(
        Arrays.asList("2021-05-17T02:05:00-07", "2021-05-18T02:05:00-07", "2021-05-19T02:05:00-07"),
        planned.stream().map(p -> strFormat(p.executionDate())).collect(Collectors.toList()));
  }

  /** Planned executions in UTC. */
  @Test
  public void testCalculatePlannedExecutionsUTC() {
    var triggers = generateTriggers("5 2 * * *", "2021-05-17T00:00:00Z", "UTC", null);
    Date endDate = parseDate(triggers, Duration.ofDays(3));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(
        Arrays.asList("2021-05-16T19:05:00-07", "2021-05-17T19:05:00-07", "2021-05-18T19:05:00-07"),
        planned.stream().map(p -> strFormat(p.executionDate())).collect(Collectors.toList()));
  }

  /** Test multiple planned executions, should account for trigger time and sorted. */
  @Test
  public void testCalculatePlannedExecutionsMultiple() {
    var triggers =
        List.of(
            generateTrigger("5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null),
            generateTrigger("5,15 2,3 * * *", "2021-05-18T00:00:00-07:00", US_PACIFIC, null));
    Date endDate = parseDate(triggers, Duration.ofDays(2));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(
        Arrays.asList(
            "2021-05-17T02:05:00-07",
            "2021-05-18T02:05:00-07",
            "2021-05-18T02:05:00-07",
            "2021-05-18T02:15:00-07",
            "2021-05-18T03:05:00-07",
            "2021-05-18T03:15:00-07"),
        planned.stream().map(p -> strFormat(p.executionDate())).collect(Collectors.toList()));
  }

  /** Construct next execution, reconcile with executed items. */
  @Test
  public void testConstructNextExecution() {
    var triggers =
        List.of(
            generateTrigger("5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, null),
            generateTrigger("5,15 2,3 * * *", "2021-05-18T00:00:00-07:00", US_PACIFIC, null));
    Date endDate = parseDate(triggers, Duration.ofDays(2));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    TimeTriggerExecution execution =
        TimeTriggerExecution.builder()
            .timeTriggersWithWatermarks(triggers)
            .workflowId("wfid")
            .workflowTriggerUuid("uuid")
            .workflowVersion("2")
            .build();
    TimeTriggerExecution newExecution = executionPlanner.constructNextExecution(execution, planned);
    assertEquals(
        ZonedDateTime.parse("2021-05-18T02:05:00-07:00").toInstant().toEpochMilli(),
        newExecution.getTimeTriggersWithWatermarks().getFirst().getLastTriggerTimestamp());
    assertEquals(
        ZonedDateTime.parse("2021-05-18T03:15:00-07:00").toInstant().toEpochMilli(),
        newExecution.getTimeTriggersWithWatermarks().get(1).getLastTriggerTimestamp());
    assertEquals("wfid", newExecution.getWorkflowId());
    assertEquals("uuid", newExecution.getWorkflowTriggerUuid());
    assertEquals("2", newExecution.getWorkflowVersion());
  }

  /** Planned executions yearly one time execution. */
  @Test
  public void testCalculatePlannedExecutionsOneTimeYearly() {
    var triggers =
        generateTriggers("0 30 2 7 12 ? 2021", "2021-12-01T00:00:00-00:00", US_PACIFIC, null);
    Date endDate = parseDate(triggers, Duration.ofDays(30));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(
        List.of("2021-12-07T02:30:00-08"),
        planned.stream().map(p -> strFormat(p.executionDate())).collect(Collectors.toList()));
  }

  /** Planned executions yearly one time execution, after execution passed. */
  @Test
  public void testCalculatePlannedExecutionsYearlyNoExecution() {
    var triggers =
        generateTriggers("0 30 2 7 12 ? 2021", "2022-01-01T00:00:00-00:00", US_PACIFIC, null);

    Date endDate = parseDate(triggers, Duration.ofDays(30));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertTrue(planned.isEmpty());
  }

  /** Planned executions with fuzzy delay. */
  @Test
  public void testCalculatePlannedExecutionsFuzzyDelay() {
    var triggers =
        generateTriggers(
            "5 2 * * *", "2021-05-17T00:00:00-07:00", US_PACIFIC, ParsableLong.of("5 hours"));
    Date endDate = parseDate(triggers, Duration.ofDays(1));
    List<PlannedTimeTriggerExecution> planned =
        executionPlanner.calculatePlannedExecutions(triggers, endDate, "test-workflow");
    assertEquals(1, planned.size());

    var dateTime = ZonedDateTime.parse("2021-05-17T02:05:00-07");
    Date minDate = Date.from(dateTime.toInstant());
    Date maxDate = Date.from(dateTime.plusHours(5).toInstant());

    assertFalse(minDate.after(planned.getFirst().executionDate()));
    assertTrue(maxDate.after(planned.getFirst().executionDate()));
  }
}
