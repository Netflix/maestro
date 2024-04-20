### SEL Examples

## Example 1: validate the expression result
In some cases, we want to validate the expression result before the following steps use it.
If the result is invalid, it is better to stop the workflow and surface a meaningful error message.

```sel
for (int i = 0; i < strParam.length; i += 1) {
  if (strParam[i].isEmpty()) {
    throw i + '-th element in the array is empty. Please check the input.';
  }
}
return String.join(',', strParam);
```


## Example 2: get dateInts of weeks between two dateInts
Assume there are two LONG type parameters (dateInt format) defined in a workflow.

Let's call them `startDateTime` (e.g. `20190101`) and `endDateTime` (e.g. `20190131`).

We want to write an expression to define a LONG_ARRAY consisting
weeks in dateInt format between startDateTime (inclusive) and endDateTime (exclusive).
If startDateTime is larger than endDateTime, return empty array.

Here is the expression:

```sel
return Util.dateIntsBetween(startDateTime, endDateTime, 7);
```

## Example 3: get dateInts of months between two dateInts
Assume there are two LONG type parameters (dateInt format) defined in a workflow.

Let's call them `startDateTime` (e.g. `20190101`) and `endDateTime` (e.g. `20190131`).

We want to write an expression to define a LONG_ARRAY consisting
months in dateInt format between startDateTime (inclusive) and endDateTime (exclusive).
If startDateTime is larger than endDateTime, return empty array.

Here is the expression:

```sel
DateTimeFormatter fmt = DateTimeFormat.forPattern('yyyyMMdd').withZone(DateTimeZone.forID('UTC'));
DateTime d1 = fmt.parseDateTime(String.valueOf(startDateTime));
DateTime d2 = fmt.parseDateTime(String.valueOf(endDateTime));

if (!d1.isBefore(d2)) {
  return new int[0];
}

int months = 12 * (d2.getYear() - d1.getYear()) + (d2.getMonthOfYear() - d1.getMonthOfYear());
months += d1.getDayOfMonth() == 1 ? 0 : -1; // adjust months for the start
months += d2.getDayOfMonth() == 1 ? 0 : 1;  // adjust months for the end

if (d1.getDayOfMonth() > 1) {
  d1 = d1.withDayOfMonth(1).plusMonths(1);  // adjust the start month
}

int[] ret = new int[months];
for(int i=0; i < months; i+=1) {
   ret[i] = Integer.valueOf(d1.plusMonths(i).toString("yyyyMMdd"));
}
return ret;
```

## Example 4: output the current time to a specific format
Here is the expression to output the current time to default python string format

```sel
new DateTime(System.currentTimeMillis()).withZone(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss.SSSSSS");
```

## Example 5: get the string of quarter of the year of the current time
The Joda time library does not directly support to output a quarter of year string.
Here is the SEL expression to get it for the current time, e.g. `Q3-2021`:

```sel
DateTime dt = new DateTime().withZone(DateTimeZone.forID('US/Pacific'));
return String.format('Q%s-%s', ((dt.getMonthOfYear() - 1) / 3) + 1, dt.getYear());
```

...
