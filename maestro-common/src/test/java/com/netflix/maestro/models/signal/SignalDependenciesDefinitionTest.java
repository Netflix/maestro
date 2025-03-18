package com.netflix.maestro.models.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.io.IOException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for SignalDependenciesDefinition class.
 *
 * @author jun-he
 */
public class SignalDependenciesDefinitionTest extends MaestroBaseTest {
  private SignalDependenciesDefinition definition;

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Before
  public void before() throws IOException {
    definition =
        loadObject(
            "fixtures/signal/sample-signal-dependencies-definition.json",
            SignalDependenciesDefinition.class);
  }

  @Test
  public void shouldSerDe() throws IOException {
    String ser1 = MAPPER.writeValueAsString(definition);
    SignalDependenciesDefinition actual =
        MAPPER.readValue(MAPPER.writeValueAsString(definition), SignalDependenciesDefinition.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(definition, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void shouldInitializeWithPendingStatus() {
    assertThat(definition.definitions())
        .hasSize(1)
        .first()
        .matches(i -> i.getMatchParams().get("param_a").getParam().getValue().equals("test123"));
  }
}
