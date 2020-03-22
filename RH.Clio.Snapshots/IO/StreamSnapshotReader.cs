using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace RH.Clio.Snapshots.IO
{
    public class StreamSnapshotReader : StringSnapshotReader
    {
        private readonly StreamReader _snapshotReader;
        private readonly bool _leaveOpen;

        public StreamSnapshotReader(StreamReader snapshotReader)
            : this(snapshotReader, false) { }

        public StreamSnapshotReader(StreamReader snapshotReader, bool leaveOpen)
        {
            _snapshotReader = snapshotReader ?? throw new ArgumentNullException(nameof(snapshotReader));
            _leaveOpen = leaveOpen;
        }

        protected override async Task ReceiveDocumentsAsync(ITargetBlock<string> queue, CancellationToken cancellationToken)
        {
            var documentEnumerator = new StreamEnumerator(_snapshotReader, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            while (await documentEnumerator.MoveNextAsync())
            {
                await queue.SendAsync(documentEnumerator.Current, cancellationToken);
            }

            queue.Complete();
        }

        public override void Close()
        {
            _snapshotReader.Close();
        }

        public override void Dispose()
        {
            if (!_leaveOpen)
                _snapshotReader.Dispose();
        }

        private class StreamEnumerator : IAsyncEnumerator<string>
        {
            private readonly StreamReader _snapshotReader;
            private readonly CancellationToken _cancellationToken;

            private string? _current;

            public StreamEnumerator(StreamReader snapshotReader, CancellationToken cancellationToken)
            {
                _snapshotReader = snapshotReader;
                _cancellationToken = cancellationToken;
            }

            public string Current
            {
                get
                {
                    if (_current is null)
                        throw new InvalidOperationException("Cannot get Current until MoveNextAsync() returns true.");

                    return _current;
                }
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();

                _current = null;

                while (!_snapshotReader.EndOfStream)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    var line = await _snapshotReader.ReadLineAsync();
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        _current = line;
                        return true;
                    }
                }

                return false;
            }

            public ValueTask DisposeAsync()
            {
                return new ValueTask(Task.CompletedTask);
            }
        }
    }
}
