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
        private readonly IDictionary<string, Stopwatch> _activity = new Dictionary<string, Stopwatch>();

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
                var totalWaitTimeMs = _activity.Values.Sum(sw => sw.ElapsedMilliseconds);
                var avgWaitTimeMs = (double)totalWaitTimeMs / _activity.Values.Count;

                Console.SetCursorPosition(0, _writeTopPos);

                // Clear lines
                var blankLine = new string(' ', Console.WindowWidth);
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
            }
        }

        public void OnDocumentInserted(string documentId)
        {
            Interlocked.Decrement(ref _insertingCount);
            Interlocked.Increment(ref _insertedCount);

            _activity.Remove(documentId);

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
            _activity.TryAdd(documentId, stopwatch);

            Render();
        }
    }
}
