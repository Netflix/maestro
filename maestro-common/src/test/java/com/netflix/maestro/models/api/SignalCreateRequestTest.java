package com.netflix.maestro.models.api;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for SignalCreateRequest class.
 *
 * @author jun-he
 */
public class SignalCreateRequestTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    SignalCreateRequest request =
        loadObject("fixtures/api/sample-signal-create-request.json", SignalCreateRequest.class);
    assertEquals(
        request, MAPPER.readValue(MAPPER.writeValueAsString(request), SignalCreateRequest.class));
    assertEquals(Map.of("source", Map.of("id", "test-id")), request.getPayload().get("extra"));
  }
}
