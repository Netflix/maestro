package com.netflix.maestro.models.signal;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for SignalInstance class.
 *
 * @author jun-he
 */
public class SignalInstanceTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    SignalInstance instance =
        loadObject("fixtures/signal/sample-signal-instance.json", SignalInstance.class);
    assertEquals(
        instance, MAPPER.readValue(MAPPER.writeValueAsString(instance), SignalInstance.class));
    assertEquals(Map.of("source", Map.of("id", "test-id")), instance.getPayload().get("extra"));
  }
}
