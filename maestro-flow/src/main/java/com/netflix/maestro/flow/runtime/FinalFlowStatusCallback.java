package com.netflix.maestro.flow.runtime;

import com.netflix.maestro.flow.models.Flow;

/**
 * Interface for Maestro flow engine to run final logics before finished.
 *
 * @author jun-he
 */
public interface FinalFlowStatusCallback {
  /**
   * Callback logic to run when a flow is completed.
   *
   * @param flow flow for the callback
   */
  void onFlowCompleted(Flow flow);

  /**
   * Callback logic to run when a flow is terminated.
   *
   * @param flow flow for the callback
   */
  void onFlowTerminated(Flow flow);

  /**
   * Callback logic to run when a flow is finalized.
   *
   * @param flow flow for the callback
   */
  void onFlowFinalized(Flow flow);
}
