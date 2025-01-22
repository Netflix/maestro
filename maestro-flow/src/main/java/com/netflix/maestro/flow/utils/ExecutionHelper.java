package com.netflix.maestro.flow.utils;

import com.netflix.maestro.flow.Constants;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Executor related utility class.
 *
 * @author jun-he
 */
@Slf4j
public final class ExecutionHelper {
  private ExecutionHelper() {}

  /**
   * Executor service shutdown logic.
   *
   * @param executor executor to shut down
   * @param name executor's name
   */
  public static void shutdown(ExecutorService executor, String name) {
    executor.shutdown(); // no interruption
    LOG.info("[{}] shutdown is called", name);
    try {
      if (!executor.awaitTermination(
          Constants.GRACEFUL_SHUTDOWN_TIMEOUT_IN_SECS, TimeUnit.SECONDS)) {
        LOG.info("[{}] shutdown is timed out and calling shutdownNow", name);
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.info("[{}] shutdown is interrupted and calling shutdownNow", name);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    LOG.info("[{}] shutdown is completed", name);
  }
}
