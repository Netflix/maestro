package com.netflix.maestro.models.signal;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

/**
 * Tests for SignalTriggerMatch class.
 *
 * @author jun-he
 */
public class SignalTriggerMatchTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    SignalTriggerMatch expected =
        loadObject("fixtures/sample-signal-trigger-match.json", SignalTriggerMatch.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    SignalTriggerMatch actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), SignalTriggerMatch.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
