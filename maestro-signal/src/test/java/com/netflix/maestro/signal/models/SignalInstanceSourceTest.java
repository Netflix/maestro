package com.netflix.maestro.signal.models;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

/**
 * Tests for SignalInstanceSource class.
 *
 * @author jun-he
 */
public class SignalInstanceSourceTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    SignalInstanceSource expected =
        loadObject("fixtures/sample-signal-instance-source.json", SignalInstanceSource.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    SignalInstanceSource actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), SignalInstanceSource.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
