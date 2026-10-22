/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.transformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import java.util.List;
import org.junit.Test;

public class StepTranslatorTest extends MaestroBaseTest {
  private final StepTranslator translator = new StepTranslator();

  @Test
  public void testTranslateLeafStepTypes() {
    for (StepType type : List.of(StepType.NOOP, StepType.KUBERNETES, StepType.EXTENSION)) {
      TypedStep step = new TypedStep();
      step.setId("step1");
      step.setType(type);
      var taskDef = translator.translate(step);
      assertEquals("step1", taskDef.taskReferenceName());
      assertEquals(Constants.MAESTRO_TASK_NAME, taskDef.type());
      assertNull(taskDef.joinOn());
      assertEquals(Defaults.DEFAULT_FAILURE_MODE, step.getFailureMode());
      assertEquals(0, step.getParams().size());
    }
  }

  @Test
  public void testTranslateJoinStep() {
    JoinStep step = JoinStep.of("join1", List.of("step1", "step2"));
    var taskDef = translator.translate(step);
    assertEquals("#join1", taskDef.taskReferenceName());
    assertEquals(StepType.JOIN.name(), taskDef.type());
    assertEquals(List.of("step1", "step2"), taskDef.joinOn());
  }

  @Test
  public void testTranslateNullStep() {
    AssertHelper.assertThrows(
        "null step should throw",
        NullPointerException.class,
        "Cannot find step in the definition",
        () -> translator.translate(null));
  }
}
