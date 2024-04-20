# SEL Statements

## Statement types

### block statement
```sel
{
  x = 10; 
  x += 10; 
}
```

### expression statement
```sel
x = 10; 
```

### if statement 
* `if (condition) ... else if (condition) ... else ...`
* condition is an expression with boolean result
* the statement can be a multi-statement block or a single statement
* else part is optional
```sel
if (2 > 1) x = 10; else x = -10;
int x=10; if(x>0) x+=10; else x-=10; return x;
```

### while statement
* `while(condition) ...`
* condition is an expression with a boolean result
* the statement can be a multi-statement block or a single statement
```sel
int x = 0; while(x<10) x+=3; return x;
```

### for statement
* `for(initialization, condition, update) ...`
* if cond is missing, it is always true.
```sel
for(;;) {} is an infinite loop 
for(x=0; x<10; x+=1) x+=1; return x;
```

### break statement
* break will exit the for/while loop
```sel
for(x=0; x<10; x+=1) break; return x;
```

### continue statement
* continue will skip the remaining statements in the same iteration
```sel
for(x=0; x<10; x+=1) {x+=10; continue; x+=10;} return x;
``` 

### return statement
* return the result of the whole expression.
```sel
for(x=0; x<10; x+=1) {x+=10; return x; x+=10;}
```

### throw statement
* throw an error in a string representation and return it.
```sel
throw "invalid variable value: " + x;
```
