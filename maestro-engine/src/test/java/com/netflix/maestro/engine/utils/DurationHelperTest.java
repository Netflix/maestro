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
package com.netflix.maestro.engine.utils;

import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class DurationHelperTest {
  @Test
  public void shouldPrintHumanReadableDuration() {
    Assert.assertEquals("0s", DurationHelper.humanReadableFormat(Duration.ZERO));
    Assert.assertEquals("24h", DurationHelper.humanReadableFormat(Duration.ofDays(1)));
    Assert.assertEquals(
        "29h 5m 10s",
        DurationHelper.humanReadableFormat(
            Duration.ofDays(1).plusHours(5).plusMinutes(5).plusSeconds(10)));
    Assert.assertEquals("48h", DurationHelper.humanReadableFormat(Duration.ofDays(2)));
    Assert.assertEquals("1h", DurationHelper.humanReadableFormat(Duration.ofSeconds(3600)));
    Assert.assertEquals("1h 1m 20s", DurationHelper.humanReadableFormat(Duration.ofSeconds(3680)));
    Assert.assertEquals("15h", DurationHelper.humanReadableFormat(Duration.ofMinutes(900)));
  }
}
