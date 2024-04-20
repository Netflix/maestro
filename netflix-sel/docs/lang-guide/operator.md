# SEL operators

SEL supports most of Java operators, including

* `?:`
```sel
x = 2 > 1 ? 10 : -10;       // x's value is number 10.
```

* `>`
```sel
x = 2 > 1;                  // x's value is boolean true.
```

* `<`
```sel
x = 2 < 1;                  // x's value is boolean false.
```

* `!`
```sel
x = !true;                  // x's value is boolean false.
```

* `==`
```sel
x = 2 == 1;                 // x's value is boolean false.
```
 
* `<=`
```sel
x = 2 <= 1;                 // x's value is boolean false.
```

* `>=`
```sel
x = 2 >= 1;                 // x's value is boolean true.
```
 
* `!=`
```sel
x = 2 != 1;                 // x's value is boolean true.
```

* `||`
```sel
x = true || false;          // x's value is boolean true.
```

* `&&`
```sel
x = true && false;          // x's value is boolean false.
```

* `+`
```sel
x = 1 + 2;                  // x's value is number 3.
```

* `-`
```sel
x = 1 - 2;                  // x's value is number -1.
```

* `*`
```sel
x = 1 * 2;                  // x's value is number 2.
```

* `/`
```sel
x = 1 / 2;                  // x's value is number 0.
x = 1.0 / 2;                // x's value is number 0.5.
```

* `%`
```sel
x = 1 % 2;                  // x's value is number 1.
```

* `+=`
```sel
x = 1;
x += 2;                     // x's value is number 3.
```
 
* `-=`
```sel
x = 1;
x -= 2;                     // x's value is number -1.
```

* `*=`
```sel
x = 1;
x *= 2;                     // x's value is number 2.
```

* `/=`
```sel
x = 1;
x /= 2;                     // x's value is number 0.
```

* `%=`
```sel
x = 1;
x %= 2;                     // x's value is number 1.
```

## Notes
* SEL does not do implicit conversion, e.g.
    * not support String multiply another number

* Comparison between String should call String method `compareTo`
    * e.g. `"hello'.compareTo('world') > 0`
