package com.netflix.maestro.models.signal;

import com.netflix.maestro.models.trigger.SignalTrigger;

/**
 * Data model for a signal trigger definition. Won't be persisted.
 *
 * @param workflowId workflow id
 * @param triggerUuid trigger uuid
 * @param signalTrigger signal trigger
 * @author jun-he
 */
public record SignalTriggerDef(
    String workflowId, String triggerUuid, SignalTrigger signalTrigger) {}
