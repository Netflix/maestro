package com.netflix.maestro.models.signal;

import java.util.List;

/**
 * Dto object to pass around signal match info. Won't be persisted.
 *
 * @param signalName signal name
 * @param paramMatches params to match
 * @author jun-he
 */
public record SignalMatchDto(String signalName, List<ParamMatchDto> paramMatches) {
  public boolean withParams() {
    return paramMatches != null && !paramMatches.isEmpty();
  }

  /**
   * Signal param to match.
   *
   * @param paramName signal param name
   * @param paramValue long or string type signal param value
   * @param operator signal operator
   */
  public record ParamMatchDto(
      String paramName, SignalParamValue paramValue, SignalOperator operator)
      implements Comparable<ParamMatchDto> {
    @Override
    public int compareTo(ParamMatchDto o) {
      return operator.getOrder() - o.operator.getOrder();
    }
  }
}
