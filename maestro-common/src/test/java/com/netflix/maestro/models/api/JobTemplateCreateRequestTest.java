package com.netflix.maestro.models.api;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import java.util.List;
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

  @Test
  public void testRoundTripSerdeWithSteps() throws Exception {
    JobTemplateCreateRequest request =
        loadObject(
            "fixtures/api/sample-job-template-with-steps-create-request.json",
            JobTemplateCreateRequest.class);
    assertEquals(StepType.TEMPLATE, request.getDefinition().getStepType());
    assertEquals(
        List.of("write", "audit"),
        request.getDefinition().getSteps().stream().map(Step::getId).toList());
    assertEquals(
        request,
        MAPPER.readValue(MAPPER.writeValueAsString(request), JobTemplateCreateRequest.class));
  }
}
