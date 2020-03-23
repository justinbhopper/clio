using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace RH.Clio.Snapshots.IO
{
    public class StreamSnapshotWriter : StringSnapshotWriter
    {
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1);
        private readonly StreamWriter _snapshotWriter;
        private readonly bool _leaveOpen;

        public StreamSnapshotWriter(StreamWriter snapshotWriter)
            : this(snapshotWriter, false) { }

        public StreamSnapshotWriter(StreamWriter snapshotWriter, bool leaveOpen)
        {
            _snapshotWriter = snapshotWriter;
            _leaveOpen = leaveOpen;
        }

        protected override async Task AppendDocumentsAsync(IReceivableSourceBlock<string> documents, CancellationToken cancellationToken)
        {
            // We do not support concurrent writes to streams, so just loop one at a time
            while (await documents.OutputAvailableAsync(cancellationToken))
            {
                var document = await documents.ReceiveAsync(cancellationToken);
                await AppendDocumentAsync(document, cancellationToken);
            }
        }

        protected override async Task AppendDocumentsAsync(IEnumerable<string> documents, CancellationToken cancellationToken)
        {
            // We do not support concurrent writes to streams, so just loop one at a time
            foreach (var document in documents)
            {
                await AppendDocumentAsync(document, cancellationToken);
            }
        }

        private async Task AppendDocumentAsync(string document, CancellationToken cancellationToken)
        {
            await _writeLock.WaitAsync();

            try
            {
                await _snapshotWriter.WriteLineAsync(document.ToCharArray(), cancellationToken);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public override void Close()
        {
            _snapshotWriter.Close();
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_leaveOpen)
                await _snapshotWriter.DisposeAsync();
        }
    }
}
