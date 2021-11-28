// The MIT License (MIT)

// Copyright (c) 2016 Jari Pennanen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using System.Collections.Concurrent;
using Xunit;

public class TaskQueue
{
    public class QueueLimitReachedException : Exception
    {
    }

    private readonly ConcurrentQueue<(CancellationTokenSource, Func<CancellationToken, Task>)> _processingQueue = new();
    private readonly ConcurrentDictionary<int, (Task, CancellationTokenSource)> _runningTasks = new();
    private readonly int _maxParallelizationCount;
    private readonly int _maxQueueLength;
    private TaskCompletionSource<bool> _tscQueue = new();

    public TaskQueue(int? maxParallelizationCount = null, int? maxQueueLength = null)
    {
        _maxParallelizationCount = maxParallelizationCount ?? int.MaxValue;
        _maxQueueLength = maxQueueLength ?? int.MaxValue;
    }

    public void Queue(Func<Task> futureTask)
    {
        if (_processingQueue.Count >= _maxQueueLength)
        {
            throw new QueueLimitReachedException();
        }
        _processingQueue.Enqueue((new CancellationTokenSource(), (c) => futureTask.Invoke()));
    }

    public CancellationTokenSource Queue(Func<CancellationToken, Task> futureTask)
    {
        if (_processingQueue.Count >= _maxQueueLength)
        {
            throw new QueueLimitReachedException();
        }
        var cancelSource = new CancellationTokenSource();
        _processingQueue.Enqueue((cancelSource, futureTask));
        return cancelSource;
    }

    public int GetQueueCount()
    {
        return _processingQueue.Count;
    }

    public int GetRunningCount()
    {
        return _runningTasks.Count;
    }

    public void Cancel()
    {
        // Set queued tokens cancelled
        foreach (var (c, _) in _processingQueue)
        {
            c.Cancel();
        }

        // Clear the queue
        _processingQueue.Clear();

        // Cancel all running tasks
        foreach (var (_, (_, cancelSource)) in _runningTasks)
        {
            cancelSource.Cancel();
        }
    }

    public void Cancel(TimeSpan delay)
    {
        // TODO: No tests yet, but it should work just as regular Cancel does.

        // Set queued tokens cancelled
        foreach (var (c, _) in _processingQueue)
        {
            c.CancelAfter(delay);
        }

        // Clear the queue
        _processingQueue.Clear();

        // Cancel all running tasks
        foreach (var (_, (_, cancelSource)) in _runningTasks)
        {
            cancelSource.CancelAfter(delay);
        }
    }

    public async Task Process()
    {
        var t = _tscQueue.Task;
        StartTasks();
        await t;
    }

    public void ProcessBackground(Action<Exception>? exception = null)
    {
        Task.Run(Process).ContinueWith(t =>
        {
            // OnlyOnFaulted guarentees Task.Exception is not null
            exception?.Invoke(t.Exception!);
        }, TaskContinuationOptions.OnlyOnFaulted);
    }

    private void StartTasks()
    {
        var startMaxCount = _maxParallelizationCount - _runningTasks.Count;
        for (int i = 0; i < startMaxCount; i++)
        {
            if (!_processingQueue.TryDequeue(out var tokenFutureTask))
            {
                // Queue is most likely empty
                break;
            }

            var (cancelSource, futureTask) = tokenFutureTask;
            var t = Task.Run(() => futureTask.Invoke(cancelSource.Token), cancelSource.Token);
            if (!_runningTasks.TryAdd(t.GetHashCode(), (t, cancelSource)))
            {
                throw new Exception("Should not happen, hash codes are unique");
            }

            t.ContinueWith((t2) =>
            {
                if (!_runningTasks.TryRemove(t2.GetHashCode(), out var _temp))
                {
                    throw new Exception("Should not happen, hash codes are unique");
                }

                // Continue the queue processing
                StartTasks();
            });
        }

        if (_processingQueue.IsEmpty && _runningTasks.IsEmpty)
        {
            // Interlocked.Exchange might not be necessary
            var _oldQueue = Interlocked.Exchange(
                ref _tscQueue, new TaskCompletionSource<bool>());
            _oldQueue.TrySetResult(true);
        }
    }
}

public class Tests
{
    [Fact]
    public async Task TestCancellation()
    {
        var n = 0;
        var t = new TaskQueue();
        var source = t.Queue(async (cancel) =>
        {
            await Task.Delay(80, cancel);
            n += 1;
        });

        // Start processing the queue
        t.ProcessBackground();

        // Cancel the task after 40 ms
        await Task.Delay(40);
        source.Cancel();

        // Wait for queue to empty
        await t.Process();

        // The n+=1 did not run
        Assert.Equal(0, n);
    }

    [Fact]
    public void TestCancellationQueued()
    {
        var n = 0;
        var t = new TaskQueue();
        var c1 = t.Queue(async (cancel) =>
        {
            await Task.Delay(80, cancel);
            n += 1;
        });
        var c2 = t.Queue(async (cancel) =>
        {
            await Task.Delay(120, cancel);
            n += 1;
        });

        Assert.False(c1.IsCancellationRequested);
        Assert.False(c2.IsCancellationRequested);
        t.Cancel();
        Assert.True(c1.IsCancellationRequested);
        Assert.True(c2.IsCancellationRequested);
    }

    [Fact]
    public async Task TestCancellationRunningTasks()
    {
        var n = 0;
        var t = new TaskQueue();
        var c1 = t.Queue(async (cancel) =>
        {
            await Task.Delay(80, cancel);
            n += 1;
        });
        var c2 = t.Queue(async (cancel) =>
        {
            await Task.Delay(120, cancel);
            n += 1;
        });

        // Start processing the queue
        t.ProcessBackground();

        // Cancel the task after 40 ms
        await Task.Delay(40);
        Assert.Equal(2, t.GetRunningCount());
        Assert.False(c1.IsCancellationRequested);
        Assert.False(c2.IsCancellationRequested);
        t.Cancel();
        Assert.True(c1.IsCancellationRequested);
        Assert.True(c2.IsCancellationRequested);

        // Cancellation should take effect shortly
        await Task.Delay(10);
        Assert.Equal(0, t.GetRunningCount());

        // Wait for queue to empty
        await t.Process();

        // The n+=1 did not run
        Assert.Equal(0, n);
    }

    [Fact]
    public async Task TestQueueLength()
    {
        var t = new TaskQueue(maxQueueLength: 2);
        t.Queue(async () => { await Task.Delay(40); });
        t.Queue(async () => { await Task.Delay(40); });
        Assert.Throws<TaskQueue.QueueLimitReachedException>(() => t.Queue(async () => { await Task.Delay(40); }));
        await t.Process();
    }

    [Fact]
    public async Task TestMaxParallelization()
    {
        var t = new TaskQueue(maxParallelizationCount: 4);
        var n = 0;
        // Sequential delays should ensure that tasks complete in order for
        // `n` to grow linearly
        t.Queue(async () => { await Task.Delay(40); n++; });
        t.Queue(async () => { await Task.Delay(50); n++; });
        t.Queue(async () => { await Task.Delay(60); n++; });
        t.Queue(async () => { await Task.Delay(70); n++; });

        // Following are queued and will be run as above tasks complete
        // Task delay for the first must be 40 because 40 + 40 > 70 
        t.Queue(async () => { await Task.Delay(40); n++; });
        t.Queue(async () => { await Task.Delay(50); n++; });

        // Intentionally not awaited, starts tasks asynchronously
        t.ProcessBackground();

        // Wait for tasks to start
        await Task.Delay(10);

        // Tasks should now be running
        Assert.Equal(4, t.GetRunningCount());

        await t.Process();

        // Queue and running tasks should now have ran to completion
        Assert.Equal(0, t.GetRunningCount());
        Assert.Equal(0, t.GetQueueCount());
        Assert.Equal(6, n);
    }

    [Fact]
    public async Task TestEmptyRun()
    {
        var t = new TaskQueue(maxParallelizationCount: 4);
        await t.Process();
    }
}

public class Program
{
    public static async Task DoTask(int n)
    {
        Console.WriteLine($"Processing: {n}");
        await Task.Delay(1000);
        Console.WriteLine($"Processsed: {n}");
    }

    public static void Main()
    {
        var t = new TaskQueue(maxParallelizationCount: 2, maxQueueLength: 2);
        t.Queue(() => DoTask(1)); // Runs this on 1st batch.

        // Works even without following `Wait()`, in that case the first
        // starts immediately the second near instantly following, and third
        // waits until either 1 or 2 completes.
        t.Process().Wait();
        Console.WriteLine("First batch should have ran to completion.");
        Console.WriteLine("");

        t.Queue(() => DoTask(2)); // Runs this on 2nd batch
        t.Queue(() => DoTask(3)); // Runs this on 2nd batch
        try
        {
            t.Queue(() => DoTask(4)); // Not ran, capped
        }
        catch (TaskQueue.QueueLimitReachedException)
        {
            Console.WriteLine("Queue limit reached");
        }

        t.Process().Wait();
        Console.WriteLine($"Processed second batch. Queue and running tasks should be empty.");
        Console.WriteLine($"Queue has now {t.GetQueueCount()} future tasks, and {t.GetRunningCount()} running tasks.");
        Console.WriteLine("");

        t.Queue(() => DoTask(7)); // Runs this on 2nd batch
        t.Queue(() => DoTask(8)); // Runs this on 2nd batch

        try
        {
            t.Queue(() => DoTask(9)); // Not ran, capped
        }
        catch (TaskQueue.QueueLimitReachedException)
        {
            Console.WriteLine("Queue limit reached 2");
        }
        Console.WriteLine($"Queued. Queue should have two future tasks, and nothing running yet.");
        Console.WriteLine($"Queue has now {t.GetQueueCount()} future tasks, and {t.GetRunningCount()} running tasks.");

        t.Process().Wait();
        Console.WriteLine("Completed, press enter to quit.");
        Console.ReadLine();
        Console.WriteLine("The End!");
    }
}
