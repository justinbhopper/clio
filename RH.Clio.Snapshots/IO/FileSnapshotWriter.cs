using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots.IO
{
    public class FileSnapshotWriter : ISnapshotHandle
    {
        private readonly StreamSnapshotWriter _snapshotWriter;
        private readonly FileStream _snapshotStream;
        private readonly FileStream _changeFeedStream;

        public FileSnapshotWriter(FileStream snapshotStream, FileStream changeFeedStream, Encoding encoding)
            : this(snapshotStream, changeFeedStream, encoding, false) { }

        public FileSnapshotWriter(FileStream snapshotStream, FileStream changeFeedStream, Encoding encoding, bool leaveOpen)
        {
            _snapshotStream = snapshotStream;
            _changeFeedStream = changeFeedStream;

            var snapshotStreamWriter = new StreamWriter(_snapshotStream, encoding);
            var changeFeedStreamWriter = new StreamWriter(_changeFeedStream, encoding);
            _snapshotWriter = new StreamSnapshotWriter(snapshotStreamWriter, changeFeedStreamWriter, leaveOpen);
        }

        public Task AppendSnapshotDocumentAsync(JObject document, CancellationToken cancellationToken)
        {
            return _snapshotWriter.AppendSnapshotDocumentAsync(document, cancellationToken);
        }

        public Task AppendChangeFeedDocumentAsync(JObject document, CancellationToken cancellationToken)
        {
            return _snapshotWriter.AppendSnapshotDocumentAsync(document, cancellationToken);
        }

        public Task DeleteAsync(CancellationToken cancellationToken)
        {
            File.Delete(_snapshotStream.Name);
            File.Delete(_changeFeedStream.Name);

            return Task.CompletedTask;
        }

        public void Close()
        {
            _snapshotWriter.Close();
        }

        public ValueTask DisposeAsync()
        {
            return _snapshotWriter.DisposeAsync();
        }
    }
}
