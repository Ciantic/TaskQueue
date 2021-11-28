# TaskQueue

Very simple class that implements thread-safe (hopefully) task queue that can be
awaited to complete it's tasks. Options are to set a degree of parallelization
and maximum queue length.

## Testing

Tests are not exhaustive, but you can run tests with `dotnet test` and test a
simple program with `dotnet run`.