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

        private readonly CloudAppendBlob _target;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;

        public BlobSnapshotFactory(CloudAppendBlob target, int maxConcurrency)
            : this(target, Encoding.UTF8, maxConcurrency) { }

        public BlobSnapshotFactory(CloudAppendBlob target, Encoding encoding, int maxConcurrency)
        {
            _target = target;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        public ISnapshotReader CreateReader()
        {
            return new BlobSnapshotReader(_target, _encoding, _maxConcurrency);
        }

        public async Task<ISnapshotHandle> CreateWriterAsync(bool deleteIfExists, CancellationToken cancellationToken)
        {
            if (deleteIfExists)
            {
                if (await _target.ExistsAsync(cancellationToken))
                {
                    await _target.DeleteAsync(cancellationToken);

                    // An exception can occur when trying to create a blob still being deleted
                    await s_createRetryPolicy.ExecuteAsync(_target.CreateOrReplaceAsync, cancellationToken);
                }
            }
            else if (!await _target.ExistsAsync(cancellationToken))
            {
                await _target.CreateOrReplaceAsync(cancellationToken);
            }

            return new BlobSnapshotWriter(_target, _encoding, _maxConcurrency);
        }
    }
}
