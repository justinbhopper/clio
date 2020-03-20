using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotWriter : StringSnapshotWriter, ISnapshotHandle
    {
        internal const string s_documentPrefix = "document";
        internal const string s_changeFeedPrefix = "changefeed";

        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly string _documentPrefix;
        private readonly string _changeFeedPrefix;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private int _documentIndex;
        private int _changeFeedIndex;
        private bool _initialized;

        public BlobSnapshotWriter(CloudBlobContainer container, Encoding encoding, string rootPath)
        {
            _container = container;
            _encoding = encoding;
            _documentPrefix = Path.Combine(rootPath, s_documentPrefix);
            _changeFeedPrefix = Path.Combine(rootPath, s_changeFeedPrefix);
        }

        protected override async Task AppendSnapshotDocumentAsync(string document, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var index = Interlocked.Increment(ref _documentIndex);
            await UploadAsync(_documentPrefix + "-" + index, document, cancellationToken);
        }

        protected override async Task AppendChangeFeedDocumentAsync(string document, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var index = Interlocked.Increment(ref _changeFeedIndex);
            await UploadAsync(_changeFeedPrefix + "-" + index, document, cancellationToken);
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
