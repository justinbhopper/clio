using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace RH.Clio
{
    public class DocumentWriteLogger
    {
        private readonly object _lock = new object();
        private readonly int _writeTopPos;
        private readonly Stopwatch _timer = new Stopwatch();
        private readonly IDictionary<string, Stopwatch> _duration = new Dictionary<string, Stopwatch>();

        private int _queueCount;
        private int _waitCount;
        private int _insertingCount;
        private int _insertedCount;

        public DocumentWriteLogger(int writeTopPos)
        {
            _writeTopPos = writeTopPos;
        }

        private void Render()
        {
            lock (_lock)
            {
                var totalWaitTimeMs = _duration.Values.Sum(sw => sw.ElapsedMilliseconds);
                var avgWaitTimeMs = (double)totalWaitTimeMs / _duration.Values.Count;

                Console.SetCursorPosition(0, _writeTopPos);

                // Clear lines
                var blankLine = new string(' ', Console.WindowWidth);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);

                Console.SetCursorPosition(0, _writeTopPos);

                Console.WriteLine("Queued: " + _queueCount);
                Console.WriteLine("Waiting: " + _waitCount);
                Console.WriteLine("Inserting: " + _insertingCount);
                Console.WriteLine("Inserted: " + _insertedCount);
                Console.WriteLine("Average Insert Time: {0:0.00}ms", avgWaitTimeMs);
                Console.WriteLine("Total time elapsed: {0:0.00}sec", _timer.Elapsed.TotalSeconds);
            }
        }

        public void OnDocumentInserted(string documentId)
        {
            Interlocked.Decrement(ref _insertingCount);
            Interlocked.Increment(ref _insertedCount);

            lock (_lock)
            {
                _duration.Remove(documentId);
            }

            Render();
        }

        public void OnDocumentInserting()
        {
            Interlocked.Decrement(ref _queueCount);
            Interlocked.Increment(ref _insertingCount);

            Render();
        }

        public void OnThrottleStarted()
        {
            Interlocked.Decrement(ref _insertingCount);
            Interlocked.Increment(ref _waitCount);

            Render();
        }

        public void OnThrottleFinished()
        {
            Interlocked.Decrement(ref _waitCount);
            Interlocked.Increment(ref _queueCount);

            Render();
        }

        public void OnDocumentQueued(string documentId)
        {
            Interlocked.Increment(ref _queueCount);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            lock (_lock)
            {
                _timer.Start();
                _duration.TryAdd(documentId, stopwatch);
            }

            Render();
        }
    }
}
