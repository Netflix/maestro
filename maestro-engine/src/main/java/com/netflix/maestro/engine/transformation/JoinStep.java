/*
 * Copyright 2024 Netflix, Inc.
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.StepType;
import java.util.List;
import lombok.Getter;

/** Internal wrapper class to include join step info to create MaestroGateTask. */
@Getter
class JoinStep extends AbstractStep {
  private List<String> joinOn;

  @JsonIgnore
  @Override
  public StepType getType() {
    return StepType.JOIN;
  }

  static JoinStep of(String id, List<String> joinOn) {
    JoinStep joinStep = new JoinStep();
    joinStep.joinOn = joinOn;
    joinStep.setId("#" + id);
    joinStep.setName("Gate-at-" + id);
    joinStep.setDescription("Join on " + joinOn);
    return joinStep;
  }
}
