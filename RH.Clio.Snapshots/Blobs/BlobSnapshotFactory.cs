using System.Text;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotFactory : ISnapshotFactory
    {
        private readonly CloudBlobContainer _blobContainer;
        private readonly string _databaseName;
        private readonly string _containerName;
        private readonly Encoding _encoding;

        public BlobSnapshotFactory(CloudBlobContainer blobContainer, string databaseName, string containerName)
            : this(blobContainer, databaseName, containerName, Encoding.UTF8) { }

        public BlobSnapshotFactory(CloudBlobContainer blobContainer, string databaseName, string containerName, Encoding encoding)
        {
            _blobContainer = blobContainer;
            _databaseName = databaseName;
            _containerName = containerName;
            _encoding = encoding;
        }

        public ISnapshotReader CreateReader()
        {
            return new BlobSnapshotReader(_blobContainer, _encoding, $"{_databaseName}-{_containerName}");
        }

        public ISnapshotHandle CreateWriter()
        {
            return new BlobSnapshotWriter(_blobContainer, _encoding, $"{_databaseName}-{_containerName}");
        }
    }
}
