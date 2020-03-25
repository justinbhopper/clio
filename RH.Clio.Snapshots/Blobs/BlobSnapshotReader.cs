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
        private readonly CloudAppendBlob _target;
        private readonly Encoding _encoding;
        private readonly int _maxConcurrency;

        public BlobSnapshotReader(CloudAppendBlob target, Encoding encoding, int maxConcurrency)
        {
            _target = target;
            _encoding = encoding;
            _maxConcurrency = maxConcurrency;
        }

        protected override async Task ReceiveDocumentsAsync(ITargetBlock<string> queue, CancellationToken cancellationToken)
        {
            var blobItemEnumerator = new ListBlobItemEnumerator(_container);

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
    }
}
