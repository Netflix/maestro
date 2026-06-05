/*
 * Copyright 2026 Netflix, Inc.
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

import static com.netflix.maestro.models.definition.alerting.AlertType.*;
import static com.netflix.maestro.models.definition.alerting.AlertingTypeConfig.Action.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.DefaultAlerting.PagerdutyConfig;
import com.netflix.maestro.models.definition.alerting.AlertType;
import com.netflix.maestro.models.definition.alerting.AlertingTypeConfig;
import com.netflix.maestro.models.definition.alerting.BypassDigestConfig;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link DefaultAlerting}. */
public class DefaultAlertingTest extends MaestroBaseTest {

  private DefaultAlerting expected;

  @Before
  public void setupClass() {
    AlertingTypeConfig stepFailureType = new AlertingTypeConfig();
    stepFailureType.setEmails(Collections.singleton("demo+alertconfig_failure@netflix.com"));
    stepFailureType.setPagerduties(Collections.singleton("alert config failure pager"));
    DefaultAlerting.SlackConfig sfSlack = new DefaultAlerting.SlackConfig();
    sfSlack.setChannels(Collections.singleton("channel_failure1"));
    sfSlack.setUsers(Collections.singleton("failureuser"));
    stepFailureType.setSlackConfig(sfSlack);
    stepFailureType.setActions(new HashSet<>(Arrays.asList(EMAIL, PAGE, SLACK)));

    AlertingTypeConfig tctType = new AlertingTypeConfig();
    tctType.setEmails(Collections.singleton("demo+alertconfig_tct@netflix.com"));
    tctType.setPagerduties(Collections.singleton("alert config tct pager"));
    DefaultAlerting.SlackConfig tctSlack = new DefaultAlerting.SlackConfig();
    tctSlack.setUsers(Collections.singleton("tctuser"));
    tctSlack.setChannels(Collections.singleton("channel_tct"));
    tctSlack.setMentionUsers(new HashSet<>(Arrays.asList("tct_mention1", "tct_mention2")));
    tctType.setSlackConfig(tctSlack);
    tctType.setActions(new HashSet<>(Arrays.asList(CANCEL, PAGE, SLACK)));

    AlertingTypeConfig lrType = new AlertingTypeConfig();
    lrType.setEmails(Collections.singleton("demo+alertconfig_longrunning@netflix.com"));
    lrType.setPagerduties(Collections.singleton("alert config long running pager"));
    DefaultAlerting.SlackConfig lrSlack = new DefaultAlerting.SlackConfig();
    lrSlack.setUsers(Collections.singleton("longrunninguser"));
    lrSlack.setChannels(
        new HashSet<>(Arrays.asList("channel_longrunning1", "channel_longrunning2")));
    lrType.setSlackConfig(lrSlack);
    lrType.setActions(new HashSet<>(Arrays.asList(PAGE, EMAIL)));
    lrType.setDisabled(true);
    lrType.setGranularity(AlertingTypeConfig.Granularity.ALL);

    AlertingTypeConfig bhType = new AlertingTypeConfig();
    bhType.setEmails(Collections.singleton("demo+alertconfig_breakpoint@netflix.com"));
    DefaultAlerting.SlackConfig bhSlack = new DefaultAlerting.SlackConfig();
    bhSlack.setChannels(Collections.singleton("channel_breakpoint"));
    bhType.setSlackConfig(bhSlack);
    bhType.setActions(new HashSet<>(Arrays.asList(SLACK, EMAIL)));
    bhType.setGranularity(AlertingTypeConfig.Granularity.STEP);

    AlertingTypeConfig dcType = new AlertingTypeConfig();
    dcType.setEmails(Collections.singleton("demo+alerting_wf_test_override_dc@netflix.com"));
    DefaultAlerting.SlackConfig dcSlack = new DefaultAlerting.SlackConfig();
    dcSlack.setUsers(new HashSet<>(Arrays.asList("defchange1", "defchange2")));
    dcSlack.setChannels(Collections.singleton("channel_defchange"));
    dcType.setSlackConfig(dcSlack);
    dcType.setActions(Collections.singleton(SLACK));
    dcType.setGranularity(AlertingTypeConfig.Granularity.WORKFLOW);

    expected = new DefaultAlerting();
    expected.setEmails(
        new HashSet<>(
            Arrays.asList(
                "demo+alertconfig_default1@netflix.com", "demo+alertconfig_default2@netflix.com")));
    expected.setPagerduties(new HashSet<>(Arrays.asList("default pager1", "default pager2")));
    PagerdutyConfig pagerdutyConfig = new DefaultAlerting.PagerdutyConfig();
    pagerdutyConfig.setAlwaysPage(true);
    pagerdutyConfig.setSeverity(PagerdutyConfig.Severity.WARNING);
    expected.setPagerdutyConfig(pagerdutyConfig);

    DefaultAlerting.SlackConfig defaultSlack = new DefaultAlerting.SlackConfig();
    defaultSlack.setChannels(
        new HashSet<>(Arrays.asList("default_channel_1", "default_channel_2")));
    defaultSlack.setUsers(new HashSet<>(Arrays.asList("defaultuser1", "defaultuser2")));
    expected.setSlackConfig(defaultSlack);

    BypassDigestConfig bdc = new BypassDigestConfig();
    bdc.setBypassWorkflow(true);
    bdc.setSteps(new HashSet<>(Arrays.asList("step1", "another_really_important_step")));
    expected.setBypassDigestConfig(bdc);

    expected.setTypeConfigs(new EnumMap<>(AlertType.class));
    expected.getTypeConfigs().put(STEP_FAILURE, stepFailureType);
    expected.getTypeConfigs().put(TCT_VIOLATION, tctType);
    expected.getTypeConfigs().put(LONG_RUNNING, lrType);
    expected.getTypeConfigs().put(BREAKPOINT_HIT, bhType);
    expected.getTypeConfigs().put(DEFINITION_CHANGE, dcType);

    Tct tct = new Tct();
    tct.setCompletedByHour(1);
    tct.setTz("UTC");
    expected.setTct(tct);
  }

  @Test
  public void testDeserialization() throws Exception {
    final DefaultAlerting actual =
        loadObject(
            "fixtures/alerting/workflow_alerting_all_fields_overrides_payload.json",
            DefaultAlerting.class);
    assertEquals(expected, actual);
    assertNull(actual.getTypeConfigs().get(STEP_FAILURE).getDurationMinutes());
  }

  @Test
  public void testSerde() throws Exception {
    final DefaultAlerting reparsed =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), DefaultAlerting.class);
    assertEquals(expected, reparsed);
  }

  @Test
  public void testSerdeWithSuccessWithin() throws Exception {
    AlertingTypeConfig config = new AlertingTypeConfig();
    expected.getTypeConfigs().put(SUCCESS_WITHIN, config);
    config.setDurationMinutes(2400);
    config.setEmails(Collections.singleton("alertconfig_successwithin@netflix.com"));
    config.setPagerduties(Collections.singleton("successwithin_pd"));

    final DefaultAlerting reparsed =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), DefaultAlerting.class);
    assertEquals(expected, reparsed);
  }

  @Test
  public void testDeserializationWithSuccessWithin() throws Exception {
    final DefaultAlerting actual =
        loadObject(
            "fixtures/alerting/workflow_alerting_with_success_within_payload.json",
            DefaultAlerting.class);
    assertNotNull(actual.getTypeConfigs());
    assertTrue(actual.getTypeConfigs().containsKey(SUCCESS_WITHIN));
    AlertingTypeConfig config = actual.getTypeConfigs().get(SUCCESS_WITHIN);
    assertEquals(1440, config.getDurationMinutes().intValue());
  }

  @Test
  public void testDefaultAlertingUpdate() {
    DefaultAlerting parsable = new DefaultAlerting();
    parsable.setEmails(Set.of("${foo}", "${bar}"));
    parsable.update(
        paramDefinition -> {
          Parameter param = paramDefinition.toParameter();
          if (param.getValue().equals("${foo}")) {
            param.setEvaluatedResult("test1@netflix.com");
          } else {
            return null;
          }
          param.setEvaluatedTime(1L);
          return param;
        });
    assertEquals(Set.of("test1@netflix.com", "${bar}"), parsable.getEmails());
  }
}
