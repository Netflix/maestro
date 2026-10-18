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
    TemplatePrepare prepare = expected.getDefinition().getPrepare();
    assertEquals(TemplatePrepare.STATIC, prepare.getType());
    assertEquals(3, prepare.getSteps().size());
    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        prepare.getSteps().stream().map(Step::getId).toList());
    assertEquals(
        Map.of("audit", "true"), prepare.getSteps().getFirst().getTransition().getSuccessors());
    assertEquals(
        "publish_snapshot__snapshot;",
        expected.getDefinition().getOutputParams().get("published_snapshot").getExpression());
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeWithStepPreparer() throws Exception {
    JobTemplate expected =
        loadObject("fixtures/stepruntime/job_template_with_step_preparer.json", JobTemplate.class);
    assertEquals(TemplatePrepare.STEP, expected.getDefinition().getPrepare().getType());
    assertEquals("generate", expected.getDefinition().getPrepare().getStep().getId());
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    assertEquals(expected, actual);
    assertEquals(ser1, MAPPER.writeValueAsString(actual));
  }
}
