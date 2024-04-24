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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import org.junit.Test;

public class TctTest extends MaestroBaseTest {
  @Test
  public void testGetCompletedByTsParam() {
    Tct tct = new Tct();
    tct.setCompletedByTs(123L);
    tct.setTz("UTC");
    LongParamDefinition expected =
        LongParamDefinition.builder().name("completed_by_ts").value(123L).build();

    LongParamDefinition actual = tct.getCompletedByTsParam();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetCompletedByTsParamWithCompletedByHour() {
    Tct tct = new Tct();
    tct.setCompletedByHour(1);
    tct.setTz("UTC");
    LongParamDefinition expected =
        LongParamDefinition.builder()
            .name("completed_by_ts")
            .expression(
                "tz_dateint_formatter = DateTimeFormat.forPattern('yyyyMMdd').withZone(DateTimeZone.forID('UTC'));"
                    + "dt = tz_dateint_formatter.parseDateTime(TARGET_RUN_DATE).plusHours(1).minusSeconds(1);"
                    + "return dt.getMillis();")
            .build();

    LongParamDefinition actual = tct.getCompletedByTsParam();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetCompletedByTsParamWithCompletedByHourWithoutTz() {
    Tct tct = new Tct();
    tct.setCompletedByHour(1);
    LongParamDefinition expected =
        LongParamDefinition.builder()
            .name("completed_by_ts")
            .expression(
                "tz_dateint_formatter = DateTimeFormat.forPattern('yyyyMMdd').withZone(DateTimeZone.forID(WORKFLOW_CRON_TIMEZONE));"
                    + "dt = tz_dateint_formatter.parseDateTime(TARGET_RUN_DATE).plusHours(1).minusSeconds(1);"
                    + "return dt.getMillis();")
            .build();

    LongParamDefinition actual = tct.getCompletedByTsParam();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetCompletedByTsParamWithDurationMinutes() {
    Tct tct = new Tct();
    tct.setDurationMinutes(60);
    tct.setTz("UTC");
    LongParamDefinition expected =
        LongParamDefinition.builder()
            .name("completed_by_ts")
            .expression("return new DateTime(RUN_TS).plusMinutes(60).getMillis();")
            .build();

    LongParamDefinition actual = tct.getCompletedByTsParam();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetCompletedByTsParamError() {
    Tct tct = new Tct();
    AssertHelper.assertThrows(
        "Invalid TCT definition",
        MaestroInternalError.class,
        "Invalid TCT definition, neither of time fields is set",
        tct::getCompletedByTsParam);
  }
}
