using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotWriter : StringSnapshotWriter, ISnapshotHandle
    {
        private readonly CloudAppendBlob _target;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private bool _initialized;

        public BlobSnapshotWriter(CloudAppendBlob target, Encoding encoding, int maxConcurrency)
        {
            _target = target;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        protected override async Task AppendDocumentsAsync(IReceivableSourceBlock<string> documents, CancellationToken cancellationToken)
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

        private async Task QueueDocumentsAsync(ITargetBlock<string> queue, IReceivableSourceBlock<string> documents, CancellationToken cancellationToken)
        {
            while (await documents.OutputAvailableAsync(cancellationToken))
            {
                var document = await documents.ReceiveAsync(cancellationToken);

                await queue.SendAsync(document, cancellationToken);
            }

            queue.Complete();
        }

        private async Task QueueDocumentsAsync(ITargetBlock<string> queue, IEnumerable<string> documents, CancellationToken cancellationToken)
        {
            foreach (var document in documents)
            {
                await queue.SendAsync(document, cancellationToken);
            }

            queue.Complete();
        }

        private async Task UploadDocumentsAsync(ISourceBlock<string> queue, CancellationToken cancellationToken)
        {
            while (await queue.OutputAvailableAsync(cancellationToken))
            {
                var document = await queue.ReceiveAsync(cancellationToken);
                await UploadAsync(document, cancellationToken);
            }
        }

        public async Task DeleteAsync(CancellationToken cancellationToken)
        {
            await _target.DeleteIfExistsAsync(cancellationToken);

            Close();
        }

        public override ValueTask DisposeAsync()
        {
            Close();

            return new ValueTask(Task.CompletedTask);
        }

        private async Task UploadAsync(string document, CancellationToken cancellationToken)
        {
            using var stream = new MemoryStream(_encoding.GetBytes(document));
            await _target.AppendBlockAsync(stream, null, cancellationToken);
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
                await _target.CreateOrReplaceAsync(cancellationToken);
                _initialized = true;
            }
            finally
            {
                _lock.Release();
            }
        }
    }
}
