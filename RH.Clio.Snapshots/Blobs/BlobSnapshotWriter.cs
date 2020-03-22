using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotWriter : StringSnapshotWriter, ISnapshotHandle
    {
        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private int _documentIndex;
        private bool _initialized;

        public BlobSnapshotWriter(CloudBlobContainer container, Encoding encoding, int maxConcurrency)
        {
            _container = container;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        protected override async Task AppendDocumentsAsync(IAsyncEnumerable<string> documents, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var queue = new BufferBlock<string>(new DataflowBlockOptions
            {
                BoundedCapacity = _maxConcurrency
            });

            var producer = QueueDocumentsAsync(queue, documents, cancellationToken);
            var consumer = UploadDocumentsAsync(queue, cancellationToken);

            await Task.WhenAll(producer, consumer, queue.Completion);
        }

        protected override async Task AppendDocumentsAsync(IEnumerable<string> documents, CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            var queue = new BufferBlock<string>(new DataflowBlockOptions
            {
                BoundedCapacity = _maxConcurrency
            });

            var producer = QueueDocumentsAsync(queue, documents, cancellationToken);
            var consumer = UploadDocumentsAsync(queue, cancellationToken);

            await Task.WhenAll(producer, consumer, queue.Completion);
        }

        private async Task QueueDocumentsAsync(BufferBlock<string> queue, IAsyncEnumerable<string> documents, CancellationToken cancellationToken)
        {
            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                await queue.SendAsync(document, cancellationToken);
            }

            queue.Complete();
        }

        private async Task QueueDocumentsAsync(BufferBlock<string> queue, IEnumerable<string> documents, CancellationToken cancellationToken)
        {
            foreach (var document in documents)
            {
                await queue.SendAsync(document, cancellationToken);
            }

            queue.Complete();
        }

        private async Task UploadDocumentsAsync(BufferBlock<string> queue, CancellationToken cancellationToken)
        {
            while (await queue.OutputAvailableAsync(cancellationToken))
            {
                var document = await queue.ReceiveAsync(cancellationToken);

                var index = Interlocked.Increment(ref _documentIndex);
                await UploadAsync(index.ToString(), document, cancellationToken);
            }
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
