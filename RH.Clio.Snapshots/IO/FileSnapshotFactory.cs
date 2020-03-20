using System.IO;
using System.Text;

namespace RH.Clio.Snapshots.IO
{
    public class FileSnapshotFactory : ISnapshotFactory
    {
        private readonly string _snapshotPath;
        private readonly string _changefeedPath;
        private readonly Encoding _encoding;

        public FileSnapshotFactory(string snapshotPath, string changefeedPath)
            : this(snapshotPath, changefeedPath, Encoding.UTF8) { }

        public FileSnapshotFactory(string snapshotPath, string changefeedPath, Encoding encoding)
        {
            _snapshotPath = snapshotPath;
            _changefeedPath = changefeedPath;
            _encoding = encoding;
        }

        public ISnapshotReader CreateReader()
        {
            var snapshotStream = File.Open(_snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var changeFeedStream = File.Open(_changefeedPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var snapshotStreamReader = new StreamReader(snapshotStream, _encoding);
            var changeFeedStreamReader = new StreamReader(changeFeedStream, _encoding);
            return new StreamSnapshotReader(snapshotStreamReader, changeFeedStreamReader, false);
        }

        public ISnapshotHandle CreateWriter()
        {
            var snapshotStream = File.Open(_snapshotPath, FileMode.Create, FileAccess.Write, FileShare.Read);
            var changeFeedStream = File.Open(_changefeedPath, FileMode.Create, FileAccess.Write, FileShare.Read);
            return new FileSnapshotWriter(snapshotStream, changeFeedStream, _encoding, false);
        }
    }
}
