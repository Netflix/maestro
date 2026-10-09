package com.netflix.maestro.models.stepruntime;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JobTemplateTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    JobTemplate expected = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    assertEquals(Map.of("foo", "bar"), actual.getMetadata().getExtraInfo());
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeWithSteps() throws Exception {
    JobTemplate expected =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class);
    assertEquals(StepType.TEMPLATE, expected.getDefinition().getStepType());
    assertEquals(3, expected.getDefinition().getSteps().size());
    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        expected.getDefinition().getSteps().stream().map(Step::getId).toList());
    assertEquals(
        Map.of("audit", "true"),
        expected.getDefinition().getSteps().getFirst().getTransition().getSuccessors());
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
