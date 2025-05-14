package com.netflix.maestro.validations;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.trigger.SignalTrigger;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SignalTrigger constraint.
 *
 * <p>This validates {@link SignalTrigger} definition.
 *
 * @author jun-he
 */
@Documented
@Constraint(validatedBy = SignalTriggerConstraint.TimeTriggerConstraintValidator.class)
@Target({ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SignalTriggerConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro workflow TimeTrigger duration validator. */
  class TimeTriggerConstraintValidator
      implements ConstraintValidator<SignalTriggerConstraint, SignalTrigger> {

    @Override
    public boolean isValid(SignalTrigger trigger, ConstraintValidatorContext context) {
      if (trigger != null && trigger.getDefinitions() != null) {
        if (signalNameSizeOverLimit(trigger, context)) {
          return false;
        }
        if (joinKeysWithDifferentSize(trigger, context)) {
          return false;
        }
        return !joinKeysUsedForMatch(trigger, context);
      }
      return true;
    }

    private boolean signalNameSizeOverLimit(
        SignalTrigger trigger, ConstraintValidatorContext context) {
      if (trigger.getDefinitions().size() > Constants.MAX_SIGNAL_NAMES_IN_A_TRIGGER) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[signal-trigger] the signal names within the signal triggers are more than the limit [%s]",
                    Constants.MAX_SIGNAL_NAMES_IN_A_TRIGGER))
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private boolean joinKeysWithDifferentSize(
        SignalTrigger trigger, ConstraintValidatorContext context) {
      long keyCount =
          trigger.getDefinitions().values().stream()
              .mapToInt(entry -> entry.getJoinKeys() == null ? 0 : entry.getJoinKeys().length)
              .distinct()
              .count();
      if (keyCount > 1) {
        context
            .buildConstraintViolationWithTemplate(
                "[signal-trigger] the join_keys lengths between signals in the signal triggers must be the same")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private boolean joinKeysUsedForMatch(
        SignalTrigger trigger, ConstraintValidatorContext context) {
      for (var entry : trigger.getDefinitions().values()) {
        if (entry.getMatchParams() != null && entry.getJoinKeys() != null) {
          for (var key : entry.getJoinKeys()) {
            if (entry.getMatchParams().containsKey(key)) {
              context
                  .buildConstraintViolationWithTemplate(
                      String.format(
                          "[signal-trigger] the join_key [%s] cannot be used in match_params at the same time",
                          key))
                  .addConstraintViolation();
              return true;
            }
          }
        }
      }
      return false;
    }
  }
}
