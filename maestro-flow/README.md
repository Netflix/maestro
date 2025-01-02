Maestro Flow
===================================
This module includes a highly optimized flow engine to power Maestro engine. Note that it is not a complete DAG engine.
It is only responsible to progress a list of parallel lists of tasks with a size fitting to the memory of a single machine.
Advanced graph patterns (conditional branching, subworkflow, foreach, etc.) are built in maestro engine by using this as
the basic building blocks.

It uses Java virtual thread feature to greatly simplify the concurrency handling.
Also note that the flow engine does not strictly follow the actor model or dataflow programming model but leverages
some of great concepts from both of them.
