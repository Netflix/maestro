package com.netflix.maestro.models.signal;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SignalOperator class.
 *
 * @author jun-he
 */
public class SignalOperatorTest {
  @Test
  public void testApply() {
    Assert.assertTrue(SignalOperator.EQUALS_TO.apply(1, 1));
    Assert.assertTrue(SignalOperator.EQUALS_TO.apply("foo", "foo"));
    Assert.assertFalse(SignalOperator.EQUALS_TO.apply(1, 2));
    Assert.assertFalse(SignalOperator.EQUALS_TO.apply("foo", "bar"));

    Assert.assertTrue(SignalOperator.GREATER_THAN.apply(2, 1));
    Assert.assertTrue(SignalOperator.GREATER_THAN.apply("foo", "bar"));
    Assert.assertFalse(SignalOperator.GREATER_THAN.apply(1, 2));
    Assert.assertFalse(SignalOperator.GREATER_THAN.apply("bar", "foo"));

    Assert.assertTrue(SignalOperator.GREATER_THAN_EQUALS_TO.apply(1, 1));
    Assert.assertTrue(SignalOperator.GREATER_THAN_EQUALS_TO.apply("foo", "foo"));
    Assert.assertFalse(SignalOperator.GREATER_THAN_EQUALS_TO.apply(1, 2));
    Assert.assertFalse(SignalOperator.GREATER_THAN_EQUALS_TO.apply("bar", "foo"));

    Assert.assertTrue(SignalOperator.LESS_THAN.apply(1, 2));
    Assert.assertTrue(SignalOperator.LESS_THAN.apply("bar", "foo"));
    Assert.assertFalse(SignalOperator.LESS_THAN.apply(2, 1));
    Assert.assertFalse(SignalOperator.LESS_THAN.apply("foo", "bar"));

    Assert.assertTrue(SignalOperator.LESS_THAN_EQUALS_TO.apply(1, 1));
    Assert.assertTrue(SignalOperator.LESS_THAN_EQUALS_TO.apply("bar", "foo"));
    Assert.assertFalse(SignalOperator.LESS_THAN_EQUALS_TO.apply(2, 1));
    Assert.assertFalse(SignalOperator.LESS_THAN_EQUALS_TO.apply("foo", "bar"));
  }
}
