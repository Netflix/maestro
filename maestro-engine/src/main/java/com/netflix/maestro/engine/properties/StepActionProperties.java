package com.netflix.maestro.engine.properties;

import lombok.Getter;
import lombok.Setter;

/**
 * Step Action properties. Please check {@link
 * com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao} about how they are used.
 */
@Getter
@Setter
public class StepActionProperties {
  private long actionTimeout;
  private long checkInterval;
}
