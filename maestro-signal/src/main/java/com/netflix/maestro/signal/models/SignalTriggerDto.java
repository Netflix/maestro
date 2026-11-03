package com.netflix.maestro.signal.models;

import com.netflix.maestro.annotations.SuppressFBWarnings;

/**
 * Data model for getting a signal trigger table record. Won't be persisted.
 *
 * @param workflowId workflow id
 * @param triggerUuid trigger uuid
 * @param definition signal trigger definition in string format
 * @param signals signal names
 * @param checkpoints corresponding signal checkpoints
 * @author jun-he
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public record SignalTriggerDto(
    String workflowId,
    String triggerUuid,
    String definition,
    String[] signals,
    Long[] checkpoints) {}
