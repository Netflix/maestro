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

## Other classes & methods
* `long System.currentTimeMillis()`

* `DateTimeConstants.SUNDAY`
