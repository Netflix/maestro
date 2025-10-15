package com.netflix.sel.ext;

/**
 * Extension Function for Util class. Note that the extension function is still executed within the
 * secured thread so it has the very strict access to resources.
 */
public interface ExtFunction {
  /**
   * Call a function from Util class.
   *
   * @param args the arguments to the method call
   * @return the result
   */
  Object call(Object... args);
}
