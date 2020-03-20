using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class BlobSnapshotReader : StringSnapshotReader
    {
        private readonly CloudBlobContainer _container;
        private readonly Encoding _encoding;
        private readonly string _rootPath;

        public BlobSnapshotReader(CloudBlobContainer container, Encoding encoding, string rootPath)
        {
            _container = container;
            _encoding = encoding;
            _rootPath = rootPath;
        }

        protected override IAsyncEnumerator<string> GetDocumentsAsync(CancellationToken cancellationToken = default)
        {
            var documentsEnumerator = new ListBlobItemEnumerator(_container, Path.Combine(_rootPath, BlobSnapshotWriter.s_documentPrefix));
            var changeFeedEnumerator = new ListBlobItemEnumerator(_container, Path.Combine(_rootPath, BlobSnapshotWriter.s_changeFeedPrefix));
            return new DualBlobListEnumerator(documentsEnumerator, changeFeedEnumerator, _encoding, cancellationToken);
        }

        private class DualBlobListEnumerator : IAsyncEnumerator<string>
        {
            private readonly ListBlobItemEnumerator _documents;
            private readonly ListBlobItemEnumerator _changeFeeds;
            private readonly Encoding _encoding;
            private readonly CancellationToken _cancellationToken;

            private string? _current;

            public DualBlobListEnumerator(
                ListBlobItemEnumerator documents,
                ListBlobItemEnumerator changeFeeds,
                Encoding encoding,
                CancellationToken cancellationToken)
            {
                _documents = documents;
                _changeFeeds = changeFeeds;
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

                    if (await _documents.MoveNextAsync() && await TrySetCurrentAsync(_documents.Current))
                        return true;
                }

                while (_changeFeeds.HasMoreResults)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    if (await _changeFeeds.MoveNextAsync() && await TrySetCurrentAsync(_changeFeeds.Current))
                        return true;
                }

                return false;
            }

            public async ValueTask DisposeAsync()
            {
                await _documents.DisposeAsync();
                await _changeFeeds.DisposeAsync();
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
