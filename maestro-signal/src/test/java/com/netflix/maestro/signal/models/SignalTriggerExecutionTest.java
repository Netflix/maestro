package com.netflix.maestro.signal.models;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

/**
 * Tests for SignalTriggerExecution class.
 *
 * @author jun-he
 */
public class SignalTriggerExecutionTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    SignalTriggerExecution expected =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    SignalTriggerExecution actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), SignalTriggerExecution.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
