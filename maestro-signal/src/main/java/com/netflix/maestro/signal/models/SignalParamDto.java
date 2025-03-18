package com.netflix.maestro.signal.models;

/**
 * Dto object to pass around signal param info. Won't be persisted.
 *
 * @param signalName signal name
 * @param paramName signal param name
 * @param encodedValue encoded signal param value
 * @author jun-he
 */
public record SignalParamDto(String signalName, String paramName, String encodedValue) {}
