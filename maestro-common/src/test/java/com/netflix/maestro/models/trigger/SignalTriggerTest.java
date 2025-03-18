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
package com.netflix.maestro.models.trigger;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class SignalTriggerTest extends MaestroBaseTest {

  @Test
  public void testSignalTriggerDeserialization() throws Exception {
    SignalTrigger def =
        loadObject("fixtures/signal_triggers/signal_trigger_simple.json", SignalTrigger.class);
    Assert.assertEquals(2, def.getDefinitions().size());
    Assert.assertEquals("updated_by", def.getDefinitions().get("signal_a").getJoinKeys()[0]);
    Assert.assertEquals("posted_by", def.getDefinitions().get("signal_b").getJoinKeys()[0]);
    Assert.assertEquals(
        "bar",
        def.getDefinitions().get("signal_a").getMatchParams().get("foo").getValue().getString());
  }
}
