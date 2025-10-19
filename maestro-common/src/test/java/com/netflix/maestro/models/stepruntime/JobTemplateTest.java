package com.netflix.maestro.models.stepruntime;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
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
}
