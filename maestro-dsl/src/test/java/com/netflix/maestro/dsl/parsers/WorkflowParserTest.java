package com.netflix.maestro.dsl.parsers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.dsl.BaseTest;
import com.netflix.maestro.dsl.DslWorkflowDef;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.definition.Criticality;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.SubworkflowStep;
import com.netflix.maestro.models.definition.WhileStep;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class WorkflowParserTest extends BaseTest {

  private WorkflowParser parser;

  @Before
  public void setup() {
    parser =
        new WorkflowParser(
            type -> {
              if ("spark".equals(type)) {
                return StepType.NOTEBOOK;
              } else if ("shell".equals(type)) {
                return StepType.KUBERNETES;
              }
              return null;
            });
  }

  @Test
  public void testWorkflowParser() throws IOException {
    var wfDef = loadObject("fixtures/sample-dsl-wf-1.yaml", DslWorkflowDef.class);
    WorkflowCreateRequest request = parser.toWorkflowCreateRequest(wfDef);

    assertEquals(Defaults.DEFAULT_WORKFLOW_ACTIVE_FLAG, request.getIsActive());
    assertNull(request.getGitInfo());
    assertTrue(request.getExtraInfo().isEmpty());

    var properties = request.getProperties();
    assertEquals(RunStrategy.Rule.PARALLEL, properties.getRunStrategy().getRule());
    assertEquals(5L, properties.getRunStrategy().getWorkflowConcurrency());

    var workflow = request.getWorkflow();
    assertEquals("sample-dsl-wf-1", workflow.getId());
    assertEquals("Sample Workflow DSL Example 1", workflow.getName());
    assertEquals(
        "A comprehensive example workflow demonstrating Maestro DSL", workflow.getDescription());
    assertEquals("\"2h\"", workflow.getTimeout().toString());
    assertEquals(1, workflow.getTags().getTags().size());
    assertEquals("example", workflow.getTags().getTags().getFirst().getName());
    assertEquals(Criticality.CRITICAL, workflow.getCriticality());
    assertEquals(4, workflow.getParams().size());
    assertEquals(
        "new DateTime(1569018000000).withZone(DateTimeZone.forID('UTC')).monthOfYear().getAsText();",
        workflow.getParams().get("month").asStringParamDef().getExpression());
    assertEquals(
        "s3://my-bucket/raw-data",
        workflow.getParams().get("data_source").asStringParamDef().getValue());
    assertEquals(
        "s3://my-bucket/processed-data",
        workflow.getParams().get("output_path").asStringParamDef().getValue());
    assertEquals(
        "1+9", workflow.getParams().get("partition_count").asLongParamDef().getExpression());

    assertEquals(2, workflow.getSteps().size());
    var step1 = workflow.getSteps().getFirst();
    assertEquals("extract_data", step1.getId());
    assertEquals("Extract Data from Source", step1.getName());
    assertEquals(StepType.NOTEBOOK, step1.getType());
    assertEquals("spark", step1.getSubType());
    assertEquals("30min", step1.getTimeout().asString());

    var step2 = workflow.getSteps().getLast();
    assertEquals("process_partitions", step2.getId());
    assertEquals("Process Data Partitions", step2.getName());
    assertEquals(StepType.FOREACH, step2.getType());
    assertNull(step2.getSubType());
    assertArrayEquals(
        new long[] {20200101, 20200102, 20200103},
        step2
            .getParams()
            .get("loop_params")
            .asMapParamDef()
            .getValue()
            .get("date")
            .asLongArrayParamDef()
            .getValue());
    assertEquals(
        "Util.intsBetween(0, 24, 1);",
        step2
            .getParams()
            .get("loop_params")
            .asMapParamDef()
            .getValue()
            .get("hour")
            .asLongArrayParamDef()
            .getExpression());

    assertEquals(1, ((ForeachStep) step2).getSteps().size());
    var step3 = ((ForeachStep) step2).getSteps().getFirst();
    assertEquals("retry_failed_partitions", step3.getId());
    assertEquals("Retry Failed Partitions", step3.getName());
    assertEquals("1h", step3.getTimeout().asString());
    assertEquals(StepType.WHILE, step3.getType());
    assertNull(step3.getSubType());
    assertEquals("attempt_cnt < 10", ((WhileStep) step3).getCondition());
    assertEquals(
        1L,
        step3
            .getParams()
            .get("loop_params")
            .asMapParamDef()
            .getValue()
            .get("attempt_cnt")
            .asLongParamDef()
            .getValue()
            .longValue());
    assertEquals("bar", step3.getParams().get("foo").asStringParamDef().getValue());
    assertEquals("bar", step3.getParams().get("foo").asStringParamDef().getValue());

    assertEquals(2, ((WhileStep) step3).getSteps().size());
    var step4 = ((WhileStep) step3).getSteps().getFirst();
    assertEquals("process_data", step4.getId());
    assertEquals("Process Data from Source", step4.getName());
    assertEquals(StepType.KUBERNETES, step4.getType());
    assertEquals("shell", step4.getSubType());

    var step5 = ((WhileStep) step3).getSteps().getLast();
    assertEquals("validation_workflow", step5.getId());
    assertEquals("Data Validation Pipeline", step5.getName());
    assertEquals(StepType.SUBWORKFLOW, step5.getType());
    assertNull(step5.getSubType());
    assertTrue(((SubworkflowStep) step5).getExplicitParams());
    assertEquals(
        "validation_workflow",
        step5.getParams().get("subworkflow_id").asStringParamDef().getValue());
    assertEquals(
        "default", step5.getParams().get("subworkflow_version").asStringParamDef().getValue());
    assertEquals("bar", step5.getParams().get("foo").asStringParamDef().getValue());
  }
}
