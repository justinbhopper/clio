using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots.IO
{
    public class FileSnapshotWriter : ISnapshotHandle
    {
        private readonly ISnapshotWriter _snapshotWriter;
        private readonly FileStream _snapshotStream;

        public FileSnapshotWriter(FileStream snapshotStream, Encoding encoding)
            : this(snapshotStream, encoding, false) { }

        public FileSnapshotWriter(FileStream snapshotStream, Encoding encoding, bool leaveOpen)
        {
            _snapshotStream = snapshotStream;

            var snapshotStreamWriter = new StreamWriter(_snapshotStream, encoding);
            _snapshotWriter = new StreamSnapshotWriter(snapshotStreamWriter, leaveOpen);
        }

        public Task AppendDocumentsAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken)
        {
            return _snapshotWriter.AppendDocumentsAsync(documents, cancellationToken);
        }

        public Task AppendDocumentsAsync(IEnumerable<JObject> documents, CancellationToken cancellationToken)
        {
            return _snapshotWriter.AppendDocumentsAsync(documents, cancellationToken);
        }

        public Task DeleteAsync(CancellationToken cancellationToken)
        {
            File.Delete(_snapshotStream.Name);

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
