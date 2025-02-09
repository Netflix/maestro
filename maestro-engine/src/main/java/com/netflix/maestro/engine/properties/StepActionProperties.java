package com.netflix.maestro.engine.properties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Step Action properties. Please check {@link
 * com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao} about how they are used.
 */
@Getter
@AllArgsConstructor
@ToString
@Builder
public class StepActionProperties {
  private final long actionTimeout;
  private final long checkInterval;
}
