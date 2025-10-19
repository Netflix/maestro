package com.netflix.maestro.models.api;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Map;
import org.junit.Test;

public class JobTemplateCreateRequestTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    JobTemplateCreateRequest request =
        loadObject(
            "fixtures/api/sample-job-template-create-request.json", JobTemplateCreateRequest.class);
    assertEquals(
        request,
        MAPPER.readValue(MAPPER.writeValueAsString(request), JobTemplateCreateRequest.class));
    assertEquals(Map.of("foo", "bar"), request.getExtraInfo());
  }
}
