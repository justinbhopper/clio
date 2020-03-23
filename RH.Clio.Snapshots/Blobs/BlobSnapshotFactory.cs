using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Polly;
using Polly.Retry;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotFactory : ISnapshotFactory
    {
        private static readonly AsyncRetryPolicy s_createRetryPolicy = Policy
            .Handle<StorageException>(ex => ex.Message.Contains("Try operation later", StringComparison.InvariantCultureIgnoreCase))
            .WaitAndRetryForeverAsync(_ => TimeSpan.FromMilliseconds(200));

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
            return new BlobSnapshotReader(_container, _encoding, _maxConcurrency);
        }

        public async Task<ISnapshotHandle> CreateWriterAsync(bool deleteIfExists, CancellationToken cancellationToken)
        {
            if (deleteIfExists)
            {
                if (await _container.ExistsAsync(cancellationToken))
                {
                    await _container.DeleteAsync(cancellationToken);

                    // An exception can occur when trying to create a container still being deleted
                    await s_createRetryPolicy.ExecuteAsync(_container.CreateAsync, cancellationToken);
                }
            }
            else
            {
                await _container.CreateAsync(cancellationToken);
            }

            return new BlobSnapshotWriter(_container, _encoding, _maxConcurrency);
        }
    }
}
