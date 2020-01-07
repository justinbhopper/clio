using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Snapshots.IO
{
    public class StreamSnapshotWriter : StringSnapshotWriter
    {
        private readonly StreamWriter _snapshotWriter;
        private readonly StreamWriter _changeFeedWriter;
        private readonly bool _leaveOpen;

        public StreamSnapshotWriter(StreamWriter snapshotWriter, StreamWriter changeFeedWriter)
            : this(snapshotWriter, changeFeedWriter, false) { }

        public StreamSnapshotWriter(StreamWriter snapshotWriter, StreamWriter changeFeedWriter, bool leaveOpen)
        {
            _snapshotWriter = snapshotWriter;
            _changeFeedWriter = changeFeedWriter;
            _leaveOpen = leaveOpen;
        }

        protected override Task AppendSnapshotDocumentAsync(string document, CancellationToken cancellationToken)
        {
            return _snapshotWriter.WriteLineAsync(document.ToCharArray(), cancellationToken);
        }

        protected override Task AppendChangeFeedDocumentAsync(string document, CancellationToken cancellationToken)
        {
            return _changeFeedWriter.WriteLineAsync(document.ToCharArray(), cancellationToken);
        }

        public override void Close()
        {
            _snapshotWriter.Close();
            _changeFeedWriter.Close();
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_leaveOpen)
            {
                await _snapshotWriter.DisposeAsync();
                await _changeFeedWriter.DisposeAsync();
            }
        }
    }
}
