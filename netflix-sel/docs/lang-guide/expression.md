# SEL Expressions

Every expression should end with a semicolon `;`.

## Expression types

### Assignment expression (`=, *=, /=, %=, +=, -=`)
    x = 1;
    x *= 2;

### Ternary conditional expression (`?:`)
    x > 0 ? x : -x;

### Relational expression (`||, &&, ==, !=, <, >, <=, >=`)
    x < 1 || x > 10;
    x > 1 && x < 10;
    x==1; x!=1;
    x > 1;

### Arithmetic expression (`+, -, *, /, %`)
    1+2;
    6/2;

### Unary expression (`+, -`)
    +x;
    -x;

### Other expressions
* literals, e.g. `"hello word"; true; null; 1; 2.3;`
* calling methods, e.g. `String.valueOf(1);`
* getting a value from array, e.g. `int[] arr = new int[]{1,2,3}; arr[0];`
* getting a value from map, e.g. `Map m = new Map(); m['foo'];`
* creating a new object, e.g. `new String("hello");`
