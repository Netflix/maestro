package com.netflix.maestro.signal.models;

/**
 * Dto object to pass around signal instance reference info. Won't be persisted.
 *
 * @param signalName signal name
 * @param signalId signal reference id
 * @author jun-he
 */
public record SignalInstanceRef(String signalName, long signalId) {}
