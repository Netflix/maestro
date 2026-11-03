package com.netflix.maestro.signal.models;

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.models.trigger.SignalTrigger;

/**
 * Data model for a signal trigger definition. Won't be persisted.
 *
 * @param workflowId workflow id
 * @param triggerUuid trigger uuid
 * @param signalTrigger signal trigger
 * @author jun-he
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public record SignalTriggerDef(
    String workflowId, String triggerUuid, SignalTrigger signalTrigger) {}
