using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace RH.Clio.Snapshots.Blobs
{
    public class ListBlobItemEnumerator : IAsyncEnumerator<IListBlobItem>
    {
        private readonly CloudBlobContainer _container;
        private readonly string _prefix;

        private Queue<IListBlobItem>? _page;
        private BlobContinuationToken? _continuationToken;
        private IListBlobItem? _current;
        private bool _initialized;

        public ListBlobItemEnumerator(CloudBlobContainer container, string prefix)
        {
            _container = container;
            _prefix = prefix;
        }

        public bool HasMoreResults => _page?.Count > 0 || !(_continuationToken is null);

        public IListBlobItem Current
        {
            get
            {
                if (_current is null)
                    throw new InvalidOperationException("Cannot get Current until MoveNextAsync() returns true.");

                return _current;
            }
        }

        public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            await InitializeAsync(cancellationToken);

            _current = null;

            if (_page == null || _page.Count <= 0)
            {
                if (_continuationToken is null)
                    return false;

                cancellationToken.ThrowIfCancellationRequested();

                var page = string.IsNullOrEmpty(_prefix)
                    ? await _container.ListBlobsSegmentedAsync(_continuationToken, cancellationToken)
                    : await _container.ListBlobsSegmentedAsync(_prefix, _continuationToken, cancellationToken);

                _continuationToken = page.ContinuationToken;
                _page = new Queue<IListBlobItem>(page.Results);
            }

            _current = _page.Dequeue();
            return true;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            return MoveNextAsync(CancellationToken.None);
        }

        public ValueTask DisposeAsync()
        {
            _continuationToken = null;
            _page = null;
            _current = null;
            return new ValueTask(Task.CompletedTask);
        }

        private async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (_initialized)
                return;

            cancellationToken.ThrowIfCancellationRequested();

            await _container.CreateIfNotExistsAsync(cancellationToken);
            _initialized = true;
        }
    }
}
