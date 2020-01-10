using System;
using System.Threading;

namespace RH.Clio
{
    public class DocumentWriteLogger
    {
        private readonly object _lock = new object();
        private readonly int _writeTopPos;

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
                Console.SetCursorPosition(0, _writeTopPos);

                // Clear lines
                var blankLine = new string(' ', Console.WindowWidth);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);
                Console.WriteLine(blankLine);

                Console.SetCursorPosition(0, _writeTopPos);

                Console.WriteLine("Queued: " + _queueCount);
                Console.WriteLine("Waiting: " + _waitCount);
                Console.WriteLine("Inserting: " + _insertingCount);
                Console.WriteLine("Inserted: " + _insertedCount);
            }
        }

        public void OnDocumentInserted()
        {
            Interlocked.Decrement(ref _insertingCount);
            Interlocked.Increment(ref _insertedCount);

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

        public void OnDocumentQueued()
        {
            Interlocked.Increment(ref _queueCount);

            Render();
        }
    }
}
