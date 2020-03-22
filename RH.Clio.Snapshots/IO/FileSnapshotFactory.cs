using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Snapshots.IO
{
    public class FileSnapshotFactory : ISnapshotFactory
    {
        private readonly string _filePath;
        private readonly Encoding _encoding;

        public FileSnapshotFactory(string filePath)
            : this(filePath, Encoding.UTF8) { }

        public FileSnapshotFactory(string filePath, Encoding encoding)
        {
            _filePath = filePath;
            _encoding = encoding;
        }

        public ISnapshotReader CreateReader()
        {
            var snapshotStream = File.Open(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var snapshotStreamReader = new StreamReader(snapshotStream, _encoding);
            return new StreamSnapshotReader(snapshotStreamReader, false);
        }

        public Task<ISnapshotHandle> CreateWriterAsync(bool deleteIfExists, CancellationToken cancellationToken)
        {
            var fileMode = deleteIfExists ? FileMode.Create : FileMode.CreateNew;
            var snapshotStream = File.Open(_filePath, fileMode, FileAccess.Write, FileShare.Read);
            var handle = new FileSnapshotWriter(snapshotStream, _encoding, false);

            return Task.FromResult<ISnapshotHandle>(handle);
        }
    }
}
