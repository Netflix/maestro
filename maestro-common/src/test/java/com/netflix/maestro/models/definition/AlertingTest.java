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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.JsonHelper;
import java.util.Set;
import java.util.function.Function;
import org.junit.Test;

/** Tests covering the pluggable {@link Alerting} interface contract. */
public class AlertingTest {

  /** Stand-in for a downstream consumer's custom {@link Alerting} implementation. */
  public static class TestConsumerAlerting implements Alerting {
    private String customField;

    public String getCustomField() {
      return customField;
    }

    public void setCustomField(String customField) {
      this.customField = customField;
    }

    @Override
    public void update(Function<ParamDefinition, Parameter> paramParser) {}
  }

  @Test
  public void defaultMapper_deserializesExistingJsonAsDefaultAlerting() throws Exception {
    String json = "{\"emails\":[\"alice@example.com\"]}";
    ObjectMapper mapper = JsonHelper.objectMapper();
    Alerting parsed = mapper.readValue(json, Alerting.class);
    assertTrue(parsed instanceof DefaultAlerting);
    assertEquals(Set.of("alice@example.com"), ((DefaultAlerting) parsed).getEmails());
  }

  @Test
  public void defaultMapper_serializedJsonHasNoExtraTypeField() throws Exception {
    DefaultAlerting alerting = new DefaultAlerting();
    alerting.setEmails(Set.of("alice@example.com"));
    ObjectMapper mapper = JsonHelper.objectMapper();
    String json = mapper.writeValueAsString(alerting);
    assertFalse("JSON should not carry a type discriminator", json.contains("\"type\""));
  }

  @Test
  public void consumerMapping_overridesDefaultAlertingDeserialization() throws Exception {
    ObjectMapper mapper = JsonHelper.objectMapper();
    SimpleModule module = new SimpleModule();
    module.addAbstractTypeMapping(Alerting.class, TestConsumerAlerting.class);
    mapper.registerModule(module);

    TestConsumerAlerting original = new TestConsumerAlerting();
    original.setCustomField("hello");
    String json = mapper.writeValueAsString(original);
    Alerting parsed = mapper.readValue(json, Alerting.class);

    assertTrue(parsed instanceof TestConsumerAlerting);
    assertEquals("hello", ((TestConsumerAlerting) parsed).getCustomField());
  }
}
