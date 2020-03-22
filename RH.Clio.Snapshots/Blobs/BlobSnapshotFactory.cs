using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotFactory : ISnapshotFactory
    {
        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;

        public BlobSnapshotFactory(CloudBlobContainer container, int maxConcurrency)
            : this(container, Encoding.UTF8, maxConcurrency) { }

        public BlobSnapshotFactory(CloudBlobContainer container, Encoding encoding, int maxConcurrency)
        {
            _container = container;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        public ISnapshotReader CreateReader()
        {
            return new BlobSnapshotReader(_container, _encoding);
        }

        public async Task<ISnapshotHandle> CreateWriterAsync(bool deleteIfExists, CancellationToken cancellationToken)
        {
            if (deleteIfExists)
                await _container.DeleteIfExistsAsync(cancellationToken);

            // Will error if container already exists
            await _container.CreateAsync(cancellationToken);

            return new BlobSnapshotWriter(_container, _encoding, _maxConcurrency);
        }
    }
}
