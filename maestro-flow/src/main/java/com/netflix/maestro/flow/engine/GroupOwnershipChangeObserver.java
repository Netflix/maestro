package com.netflix.maestro.flow.engine;

/** Observer interface, basically. */
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface GroupOwnershipChangeObserver {
  /**
   * Allows notifying observers about FlowExecutor group ownership updates (claims, losses, etc).
   *
   * @param claimedGroupId The group id that was just claimed by the FlowExecutor.
   */
  void onGroupClaimed(long claimedGroupId);
}
