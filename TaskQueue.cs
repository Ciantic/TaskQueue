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

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ConsoleApplication
{
    public class TaskQueue
    {
        private readonly ConcurrentQueue<Func<Task>> _processingQueue = new ConcurrentQueue<Func<Task>>();
        private readonly ConcurrentDictionary<int, Task> _runningTasks = new ConcurrentDictionary<int, Task>();
        private readonly int _maxParallelizationCount;
        private readonly int _maxQueueLength;
        private TaskCompletionSource<bool> _tscQueue = new TaskCompletionSource<bool>();

        public TaskQueue(int? maxParallelizationCount = null, int? maxQueueLength = null)
        {
            _maxParallelizationCount = maxParallelizationCount ?? int.MaxValue;
            _maxQueueLength = maxQueueLength ?? int.MaxValue;
        }

        public bool Queue(Func<Task> futureTask)
        {
            if (_processingQueue.Count < _maxQueueLength)
            {
                _processingQueue.Enqueue(futureTask);
                return true;
            }
            return false;
        }

        public int GetQueueCount() {
            return _processingQueue.Count; 
        }

        public int GetRunningCount() {
            return _runningTasks.Count;
        }

        public async Task Process()
        {
            var t = _tscQueue.Task;
            StartTasks();
            await t;
        }

        private void StartTasks()
        {
            var startMaxCount = _maxParallelizationCount - _runningTasks.Count;
            for (int i = 0; i < startMaxCount; i++)
            {
                Func<Task> futureTask;
                if (!_processingQueue.TryDequeue(out futureTask))
                {
                    // Queue is most likely empty
                    break;
                }

                var t = Task.Run(futureTask);
                if (!_runningTasks.TryAdd(t.GetHashCode(), t))
                {
                    throw new Exception("Should not happen, hash codes are unique");
                }

                t.ContinueWith((t2) =>
                {
                    Task _temp;
                    if (!_runningTasks.TryRemove(t2.GetHashCode(), out _temp))
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
        public static async Task DoTask(int n)
        {
            Console.WriteLine($"Processing: {n}");
            await Task.Delay(1000);
            Console.WriteLine($"Processsed: {n}");
        }

        [Fact]
        public async Task TestQueueLength()
        {
            var t = new TaskQueue(maxQueueLength: 2);
            t.Queue(async () => { await Task.Delay(40); });
            t.Queue(async () => { await Task.Delay(40); });
            t.Queue(async () => { await Task.Delay(40); }); // Dropped, not ran
            t.Queue(async () => { await Task.Delay(40); }); // Dropped, not ran
            Assert.Equal(2, t.GetQueueCount());
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
            t.Process();
            
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

        public static void Main(string[] args)
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

            t.Queue(() => DoTask(4)); // Not ran, capped
            t.Queue(() => DoTask(5)); // Not ran, capped
            t.Queue(() => DoTask(6)); // Not ran, capped
            t.Process().Wait();
            Console.WriteLine($"Processed second batch. Queue and running tasks should be empty.");
            Console.WriteLine($"Queue has now {t.GetQueueCount()} future tasks, and {t.GetRunningCount()} running tasks.");
            Console.WriteLine("");

            t.Queue(() => DoTask(7)); // Runs this on 2nd batch
            t.Queue(() => DoTask(8)); // Runs this on 2nd batch

            t.Queue(() => DoTask(9)); // Not ran, capped
            Console.WriteLine($"Queued. Queue should have two future tasks, and nothing running yet.");
            Console.WriteLine($"Queue has now {t.GetQueueCount()} future tasks, and {t.GetRunningCount()} running tasks.");

            t.Process().Wait();
            Console.WriteLine("Completed, press any key to quit.");
            Console.ReadLine();
            Console.WriteLine("The End!");
        }
    }
}
