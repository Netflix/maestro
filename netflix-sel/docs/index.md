# Welcome to SEL

## What is SEL
SEL is an expression language (EL) which evaluates Maestro parameters to create dynamic workflow execution.

It is simple, secure, and safe.

* It is a simple EL and the grammar and syntax follow JLS ([Java Language Specifications](https://docs.oracle.com/javase/specs/)). 
SEL only supports a subset of JLS as it is designed for scheduler use cases. 
For example, it does not support defining a class within a parameter expression.
* It supports permission control built upon Java security features.
* It supports runtime checks (e.g. loop iteration limit, array size check, etc.) 
* It does rigorous validations (e.g. throwing a validation error for expression `int x = "hello world"`).


## SEL Language guide

* [Getting started](lang-guide/getting-started.md)
* [Data types](lang-guide/data-type.md)
* [Operators](lang-guide/operator.md)
* [Expressions](lang-guide/expression.md)
* [Statements](lang-guide/statement.md)
* [Supported classes](lang-guide/class-function.md)
* [Examples](lang-guide/example.md)
