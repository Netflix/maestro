package com.netflix.maestro.models.api;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class StepOutputDataRequestTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    StepOutputDataRequest request =
        loadObject("fixtures/api/sample-output-data-request.json", StepOutputDataRequest.class);
    assertEquals(
        request, MAPPER.readValue(MAPPER.writeValueAsString(request), StepOutputDataRequest.class));
    assertEquals("KUBERNETES", request.getExtraInfo().get("external_job_type"));
  }
}
