using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotWriter : StringSnapshotWriter, ISnapshotHandle
    {
        internal const string s_documentPrefix = "document";
        internal const string s_changeFeedPrefix = "document";

        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private int _documentIndex;
        private int _changeFeedIndex;
        private bool _initialized;

        public BlobSnapshotWriter(CloudBlobContainer container, Encoding encoding)
        {
            _container = container;
            _encoding = encoding;
        }

        protected override async Task AppendSnapshotDocumentAsync(string document, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var index = Interlocked.Increment(ref _documentIndex);
            await UploadAsync(s_documentPrefix + "-" + index, document, cancellationToken);
        }

        protected override async Task AppendChangeFeedDocumentAsync(string document, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var index = Interlocked.Increment(ref _changeFeedIndex);
            await UploadAsync(s_changeFeedPrefix + "-" + index, document, cancellationToken);
        }

        public async Task DeleteAsync(CancellationToken cancellationToken)
        {
            await _container.DeleteIfExistsAsync(cancellationToken);

            Close();
        }

        public override ValueTask DisposeAsync()
        {
            Close();

            return new ValueTask(Task.CompletedTask);
        }

        private async Task UploadAsync(string blobName, string document, CancellationToken cancellationToken)
        {
            var blob = _container.GetBlockBlobReference(blobName);

            var bytes = _encoding.GetBytes(document);
            await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length, cancellationToken);
        }

        private async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (_initialized)
                return;

            await _lock.WaitAsync(TimeSpan.FromSeconds(10));

            // Double lock pattern
            if (_initialized)
                return;

            try
            {
                await _container.CreateIfNotExistsAsync(cancellationToken);
                _initialized = true;
            }
            finally
            {
                _lock.Release();
            }
        }
    }
}
