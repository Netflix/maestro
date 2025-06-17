# SEL Classes
In addition to the data type classes, SEL also supports the following classes.
Note that SEL currently supports a limited set of Java class methods used by the existing workflows. 
If you have any additional feature requests, please let us know.

## Math class
* `int/long min(int/long, int/long)`
```sel
return Math.min(1, 2);                  // return 1;
```

* `int/long max(int/long, int/long)`
```sel
return Math.max(1, 2);                  // return 2;
```

* `double random()`
```sel
return Math.random();                   // return a random double between 0.0 and 1.0.
```

* `double pow(int/long/double, int/long/double)`
```sel
return Math.pow(2, 3);                  // returns the double value of the first argument raised to the power of the second argument
```

## UUID class
* `UUID randomUUID()`
```sel
return UUID.randomUUID().toString();    // returns a random UUID String
```

## Joda DateTime class
* Constructors `new DateTime()`, `new DateTime(long)`, 
* `String toString()`
* `DateTime parse(String, DateTimeFormatter)`
* `DateTime withZone(DateTimeZone)`
* `DateTime minusYears(int)`
* `DateTime plusYears(int)`
* `DateTime minusMonths(int)`
* `DateTime plusMonths(int)`
* `DateTime minusWeeks(int)`
* `DateTime plusWeeks(int)`
* `DateTime minusDays(int)`
* `DateTime plusDays(int)`
* `DateTime minusHours(int)`
* `DateTime plusHours(int)`
* `DateTime minusMinutes(int)`
* `DateTime plusMinutes(int)`
* `DateTime minusSeconds(int)`
* `DateTime plusSeconds(int)`
* `DateTime minusMillis(int)`
* `DateTime plusMillis(int)`
* `Boolean isAfter(DateTime)`
* `Boolean isBefore(DateTime)`
* `Boolean isEqual(DateTime)`
* `Property monthOfYear()`
* `Property weekyear()`
* `Property weekOfWeekyear()`
* `Property dayOfYear()`
* `Property dayOfMonth()`
* `Property dayOfWeek()`
* `Property hourOfDay()`
* `Property minuteOfDay()`
* `Property minuteOfHour()`
* `Property secondOfDay()`
* `Property secondOfMinute()`
* `Property millisOfDay()`
* `Property millisOfSecond()`
* `withTimeAtStartOfDay()`
* `DateTime withYear(int)`
* `DateTime withWeekyear(int)`
* `DateTime withMonthOfYear(int)`
* `DateTime withWeekOfWeekyear(int)`
* `DateTime withDayOfYear(int)`
* `DateTime withDayOfMonth(int)`
* `DateTime withDayOfWeek(int)`
* `DateTime withHourOfDay(int)`
* `DateTime withMinuteOfHour(int)`
* `DateTime withSecondOfMinute(int)`
* `DateTime withMillisOfSecond(int)`
* `DateTime withMillisOfDay(int)`
* `long getMillis()`
* `int getYear()`
* `int getHourOfDay()`
* `int getWeekOfWeekyear()`
* `int getWeekyear()`
* `int getDayOfWeek()`
* `int getDayOfMonth()`
* `int getDayOfYear()`
* `int getMillisOfDay()`
* `int getMillisOfSecond()`
* `int getMinuteOfDay()`
* `int getMinuteOfHour()`
* `int getSecondOfMinute()`
* `int getMonthOfYear()`
* `int getSecondOfDay()`
* `DateTime toDateTime(DateTimeZone)`


## Joda DateTimeFormatter class
* `DateTimeFormatter withZone(DateTimeZone)`
* `DateTime parseDateTime(String/int/long)`
* `long parseMillis(String)`
* `DateTimeFormatter forPattern(String) `
* `String print(DateTime/long)`

## Joda DateTimeZone class
* `DateTimeZone forID(String)`
```sel
return DateTimeZone.forID('UTC');       // returns UTC DateTimeZone
```

* `int getOffset(DateTime)`
```sel
return DateTimeZone.UTC.getOffset(new DateTime());  // return the millisecond offset to add to UTC to get local time.
```

* field `UTC`
```sel
return DateTimeZone.UTC;                // returns UTC DateTimeZone
```

## Joda DateTimeDays class
* `Days daysBetween(DateTime, DateTime)`
```sel
return Days.daysBetween(dt1, dt2);      // returns the number of days between two Joda DateTime objects
```

* `int getDays()`
```sel
return days.getDays();                  // returns the number of days that this object represents.
```

## Util class
* `long dateIntToTs(String/int/long)`

* `long dateIntHourToTs(String/int/long, String/int/long, String, String/int/long, String/int/long)`
```sel
// 1st arg is dateInt, 2nd arg is hour, 3rd arg is timezone, 4th arg is day offset, 5th arg is hour offsett. 
return Util.dateIntHourToTs("20210707", "01", "UTC", 0, 0);    // returns LONG: 1625619600000.
```

* `String incrementDateInt(String/int/long, int)`

* `String tsToDateInt(String/int/long)`

* `String timeoutForDateTimeDeadline(DateTime, String)`

* `String timeoutForDateIntDeadline(String/int/long, String)`

* `long[] dateIntsBetween(String/int/long. String/int/long, String/int/long)`
```sel
return Util.dateIntsBetween(20200226, 20200303, 1);    // returns LONG_ARRAY: [20200226, 20200227, 20200228, 20200229, 20200301, 20200302].
```

* `long[] intsBetween(String/int/long. String/int/long, String/int/long)`


## Parameter related classes (params)
* `Object get(String)`
```sel
return params.get('param1');    // returns parameter param1's value.
```

* `Object [String]`
```sel
return params['param1'];    // returns parameter param1's value.
```

* `boolean containsKey(String)`
```sel
return params.containsKey('param1');    // returns true if parameter param1 exists, otherwise false.
```

* `Object getFromStep(String, String)`
```sel
return params.getFromStep('step1', 'param1');    // returns parameter param1's value from step1.
```

* `Object getFromStep(String, "MAESTRO_STEP_STATUS")`
```sel
return params.getFromStep('step1', 'MAESTRO_STEP_STATUS');    // returns the status of upstream step1.

return params.getFromStep('step1', 'MAESTRO_STEP_END_TIME');    // returns the end time of upstream step1.
```

* `Object getFromSignal(String, String)`
```sel
return params.getFromSignal('signal1', 'param1');    // returns parameter param1's value from signal1. Use it to get param from the signal triggers.
```

* `Object getFromSignalOrDefault(String, String, String)`
```sel
return params.getFromSignalOrDefault('signal1', 'param1', 'value1');    // returns parameter param1's value from signal1, if there is any exception, then return the default value (currently only support string type)
```

* `Object getFromSignalDependency(String, String)`
```sel
return params.getFromSignalDependency('signal1', 'param1');    // returns parameter param1's value from signal1. Use it to get param from the input table/signal.
```

* `Object getFromForeach(String, String, String)`
```sel
return params.getFromForeach('foreach-job1', 'foreach-step1', 'param1');    // returns an array of parameter param1's values from all steps with id=`foreach-step1` in foreach step with id=`foreach-job1`. The array index matches the iteration index.
```

* `Object getFromSubworkflow(String, String, String)`
```sel
/*
 'subworkflow-job1': the step id of subworkflow step (in parent workflow), which launches the subworkflow instance.
 'sub-step1': the step id of step in subworkflow.
 'param1': the param from sub-step1 in subworkflow.
 returns parameter param1's value from the subworkflow instance's sub-step1 step's latest attempt.
*/
return params.getFromSubworkflow('subworkflow-job1', 'sub-step1', 'param1');
```

* `long nextUniqueId()`
```sel
return params.nextUniqueId();    // generate a random unique id.
```

* `String getFromInstance(String)`
Get system defined parameters from workflow instance. Cannot be used to reference user defined workflow-level parameters.
```sel
return params.getFromInstance('INITIATOR_TIMEZONE');    // get a certain field (e.g. 'INITIATOR_TIMEZONE', 'INITIATOR_TYPE', 'RUN_POLICY', 'owner') from workflow instance data.

return params.getFromInstance('INITIATOR_RUNNER_NAME'); // For manual run, it returns the runner username. Otherwise, it throws an error.
```

## Other classes & methods
* `long System.currentTimeMillis()`

* `DateTimeConstants.SUNDAY`
