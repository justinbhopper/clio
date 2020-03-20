using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Snapshots.IO
{
    public class StreamSnapshotReader : StringSnapshotReader
    {
        private readonly StreamReader _snapshotReader;
        private readonly StreamReader _changeFeedReader;
        private readonly bool _leaveOpen;

        public StreamSnapshotReader(StreamReader snapshotReader, StreamReader changeFeedReader)
            : this(snapshotReader, changeFeedReader, false) { }

        public StreamSnapshotReader(StreamReader snapshotReader, StreamReader changeFeedReader, bool leaveOpen)
        {
            _snapshotReader = snapshotReader ?? throw new ArgumentNullException(nameof(snapshotReader));
            _changeFeedReader = changeFeedReader ?? throw new ArgumentNullException(nameof(changeFeedReader));
            _leaveOpen = leaveOpen;
        }

        protected override IAsyncEnumerator<string> GetDocumentsAsync(CancellationToken cancellationToken = default)
        {
            return new DualStreamEnumerator(_snapshotReader, _changeFeedReader, cancellationToken);
        }

        public override void Close()
        {
            _snapshotReader.Close();
            _changeFeedReader.Close();
        }

        public override void Dispose()
        {
            if (_leaveOpen)
                return;

            _snapshotReader.Dispose();
            _changeFeedReader.Dispose();
        }

        private class DualStreamEnumerator : IAsyncEnumerator<string>, IDisposable
        {
            private readonly StreamReader _snapshotReader;
            private readonly StreamReader _changeFeedReader;
            private readonly CancellationToken _cancellationToken;

            private string? _current;

            public DualStreamEnumerator(StreamReader snapshotReader, StreamReader changeFeedReader, CancellationToken cancellationToken)
            {
                _snapshotReader = snapshotReader;
                _changeFeedReader = changeFeedReader;
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

                while (!_changeFeedReader.EndOfStream)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    var line = await _changeFeedReader.ReadLineAsync();
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
                Dispose();
                return new ValueTask(Task.CompletedTask);
            }

            public void Dispose()
            {
                _snapshotReader.Dispose();
                _changeFeedReader.Dispose();
            }
        }
    }
}
