# SEL Data Types

## Literals
SEL supports three types of literals

* Integer literal
```sel
123;
```

* Double literal
```sel
12.3;
```

* String literal
```sel
"hello world";  // double quoted
'hello world';  // single quoted, define a string literal without escaping)
```

## Date types
SEL supports the same data types as Maestro, including

* String
```sel
String s = "hello world";   // defining a string variable with type info
s = "hello " + "world";     // defining a string variable without type info
```

* Long
```sel
long l = 123;               // defining a long variable with type info
Long l = 123;               // defining a long variable with type info
l = 12 - 3;                 // defining a long variable without type info
```

* Double
```sel
double d = 12.3;            // defining a double variable with type info
Double d = 12.3;            // defining a double variable with type info
d = 1 + 2.3;                // defining a double variable without type info
```

* Boolean
```sel
boolean b = true;           // defining a boolean variable with type info
Boolean b = false;          // defining a boolean variable with type info
b = 2 > 1;                  // defining a boolean variable without type info
```

* Map<String, Object>, Object is one of types SEL supports.
```sel
Map m = new Map();          // defining a Map<String, Object> variable with type info
Map m = new HashMap();      // defining a Map<String, Object> variable with type info
m = new Map();              // defining a Map<String, Object> variable without type info
m = new HashMap();          // defining a Map<String, Object> variable without type info

m.put('foo', 'bar');        // adding an <key, value> pair into the Map variable.
```

* String Array
```sel
String[] sa = new String[]{'hello', 'world'};   // defining a string array variable with type info
sa = 'hello world'.split(' ');                  // defining a string array variable with type info
```

* Long Array
```sel
long[] l = new long[]{1, 2, 3};                 // defining a long array variable with type info
Long[] l = new Long[]{1, 2, 3};                 // defining a long array variable with type info
l = new long[]{1, 2, 3};                        // defining a long array variable without type info
l = new Long[]{1, 2, 3};                        // defining a long array variable without type info
```

* Double Array
```sel
double[] d = new double[]{1.2, 3.0};            // defining a double array variable with type info
Double[] d = new double[]{1.2, 3.0};            // defining a double array variable with type info
d = new double[]{1.2, 3.0};                     // defining a double array variable without type info
d = new Double[]{1.2, 3.0};                     // defining a double array variable without type info
```

* Boolean Array
```sel
boolean[] b = new boolean[]{true, false};       // defining a boolean array variable with type info
Boolean[] b = new Boolean[]{true, false};       // defining a boolean array variable with type info
b = new boolean[]{2 > 1, 2 < 1};                // defining a boolean array variable without type info
b = new Boolean[]{2 > 1, 2 < 1};                // defining a boolean array variable without type info
```

To make it easier for users, it also supports

* int/Integer
```sel
int l = 123;                // defining a int variable with type info
Integer l = 123;            // defining a int variable with type info
l = 12 - 3;                 // defining a int variable without type info
```

* Float
```sel
float d = 12.3f;            // defining a float variable with type info
Float d = 12.3f;            // defining a float variable with type info
d = 1.0f + 2.3f;            // defining a float variable without type info
```

* int/Integer array
```sel
int[] l = new int[]{1, 2, 3};                   // defining a long array variable with type info
Integer[] l = new Integer[]{1, 2, 3};           // defining a long array variable with type info
l = new int[]{1, 2, 3};                         // defining a long array variable without type info
l = new Integer[]{1, 2, 3};                     // defining a long array variable without type info
```

* Float array
```sel
float[] d = new float[]{1.2f, 3.0f};            // defining a double array variable with type info
Float[] d = new Float[]{1.2f, 3.0f};            // defining a double array variable with type info
d = new float[]{1.2f, 3.0f};                    // defining a double array variable without type info
d = new Float[]{1.2f, 3.0f};                    // defining a double array variable without type info
```

## Notes
* SEL only supports 1 dimensional array
* Within the expression, all accessible Maestro parameter values can be obtained by referencing parameter name itself, 
e.g. `foo`, or by using a special Map object, e.g. `params['foo']`.
