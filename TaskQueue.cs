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
using System.Threading.Tasks;

namespace ConsoleApplication
{
    public class TaskQueue
    {
        private readonly ConcurrentQueue<Func<Task>> _processingQueue = new ConcurrentQueue<Func<Task>>();
        private readonly ConcurrentDictionary<int, Task> _runningTasks = new ConcurrentDictionary<int, Task>();
        private readonly int _maxParallelizationCount;
        private readonly int _maxQueueLength;
        private Task _runningQueueTask;
        private object _lockRunningQueueTask = new object();

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

        public async Task Process()
        {
            lock (_lockRunningQueueTask)
            {
                if (_runningQueueTask == null)
                {
                    _runningQueueTask = StartTasks();
                }
            }
            await _runningQueueTask;
            lock (_lockRunningQueueTask)
            {
                _runningQueueTask = null;
            }
            Console.WriteLine("- empty queue");
        }

        private Task StartTasks(TaskCompletionSource<bool> tsc = null)
        {
            if (tsc == null)
            {
                tsc = new TaskCompletionSource<bool>();
            }

            if (_processingQueue.IsEmpty && _runningTasks.IsEmpty)
            {
                tsc.SetResult(true);
                return tsc.Task;
            }

            var startMaxCount = _maxParallelizationCount - _runningTasks.Count;
            for (int i = 0; i < startMaxCount && _processingQueue.Count > 0; i++)
            {
                Func<Task> futureTask;
                if (_processingQueue.TryDequeue(out futureTask))
                {
                    var t = Task.Run(futureTask);
                    // Console.WriteLine("Started {0}", t.GetHashCode());
                    if (_runningTasks.TryAdd(t.GetHashCode(), t))
                    {
                        t.ContinueWith((t2) =>
                        {
                            Task _temp;
                            _runningTasks.TryRemove(t2.GetHashCode(), out _temp);
                            // Console.WriteLine("Completed {0}", t2.GetHashCode());

                            // Continue the queue processing
                            StartTasks(tsc);
                        });
                    }
                    else
                    {
                        throw new Exception("Should not happen");
                    }
                }
            }
            return tsc.Task;
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
            t.Queue(() => DoTask(1)); // Runs this on 1st batch
            t.Process().Wait();       // Works even without `Wait()`

            t.Queue(() => DoTask(2)); // Runs this on 2nd batch
            t.Queue(() => DoTask(3)); // Runs this on 2nd batch

            t.Queue(() => DoTask(4)); // Not ran, capped
            t.Queue(() => DoTask(5)); // Not ran, capped
            t.Queue(() => DoTask(6)); // Not ran, capped
            t.Process().Wait();

            t.Queue(() => DoTask(7)); // Runs this on 2nd batch
            t.Queue(() => DoTask(8)); // Runs this on 2nd batch

            t.Queue(() => DoTask(9)); // Not ran, capped

            t.Process().Wait();
            Console.ReadLine();
            Console.WriteLine("The End!");
        }
    }
}
