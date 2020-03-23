using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotReader : StringSnapshotReader
    {
        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;

        public BlobSnapshotReader(CloudBlobContainer container, Encoding encoding, int maxConcurrency)
        {
            _container = container;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        protected override async Task ReceiveDocumentsAsync(ITargetBlock<string> queue, CancellationToken cancellationToken)
        {
            var blobItemEnumerator = new ListBlobItemEnumerator(_container);
            var documentEnumerator = new DocumentEnumerator(blobItemEnumerator, _encoding, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            var converter = new TransformBlock<IListBlobItem, string>(DownloadBlobAsync, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrency
            });

            var filter = new BufferBlock<string>();
            filter.LinkTo(queue, new DataflowLinkOptions
            {
                PropagateCompletion = true
            }, d => !string.IsNullOrEmpty(d));

            // Discard invalid blobs
            filter.LinkTo(DataflowBlock.NullTarget<string>());

            converter.LinkTo(filter, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });

            while (await blobItemEnumerator.MoveNextAsync())
            {
                await converter.SendAsync(blobItemEnumerator.Current, cancellationToken);
            }

            converter.Complete();
        }

        private async Task<string> DownloadBlobAsync(IListBlobItem item)
        {
            if (!(item is CloudBlockBlob blob))
                return string.Empty;

            var data = await blob.DownloadTextAsync(_encoding, AccessCondition.GenerateEmptyCondition(), new BlobRequestOptions(), new OperationContext());
            return data ?? string.Empty;
        }

        private class DocumentEnumerator : IAsyncEnumerator<string>
        {
            private readonly ListBlobItemEnumerator _documents;
            private readonly Encoding _encoding;
            private readonly CancellationToken _cancellationToken;

            private string? _current;

            public DocumentEnumerator(
                ListBlobItemEnumerator documents,
                Encoding encoding,
                CancellationToken cancellationToken)
            {
                _documents = documents;
                _encoding = encoding;
                _cancellationToken = cancellationToken;
            }

            public string Current
            {
                get
                {
                    if (_current is null)
                        throw new InvalidOperationException("Cannot get Current until MoveNextAsync() returns true.");

                    return _current;
                }
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();
                _current = null;

                while (_documents.HasMoreResults)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    if (await _documents.MoveNextAsync(_cancellationToken) && await TrySetCurrentAsync(_documents.Current))
                        return true;
                }

                return false;
            }

            public async ValueTask DisposeAsync()
            {
                await _documents.DisposeAsync();
            }

            private async Task<bool> TrySetCurrentAsync(IListBlobItem item)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                if (!(item is CloudBlockBlob blob))
                    return false;

                var data = await blob.DownloadTextAsync(_encoding, AccessCondition.GenerateEmptyCondition(), new BlobRequestOptions(), new OperationContext(), _cancellationToken);

                if (data is null)
                    return false;

                _current = data;
                return true;
            }
        }
    }
}
