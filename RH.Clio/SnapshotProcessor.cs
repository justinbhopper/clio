using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots;

namespace RH.Clio
{
    public class SnapshotProcessor : ISnapshotProcessor
    {
        private readonly ChangeFeedProcessor _changeFeedProcessor;
        private readonly ISnapshotHandle _snapshot;
        private readonly QueryDefinition _documentsQuery;
        private readonly IContainerReader _containerReader;
        private readonly ManualResetEvent _wait = new ManualResetEvent(false);
        private readonly CancellationTokenSource _cancelSource = new CancellationTokenSource();

        private Task? _runTask;

        public SnapshotProcessor(
            Container sourceContainer,
            Container leaseContainer,
            ISnapshotHandle snapshot,
            QueryDefinition documentsQuery,
            IContainerReader containerReader)
        {
            _changeFeedProcessor = sourceContainer.GetChangeFeedProcessorBuilder<dynamic>("Clio", OnChangesAsync)
                .WithInstanceName($"Clio-{Guid.NewGuid()}")
                .WithLeaseContainer(leaseContainer)
                .Build();

            _snapshot = snapshot;
            _documentsQuery = documentsQuery;
            _containerReader = containerReader;
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (!(_runTask is null))
                return;

            await _changeFeedProcessor.StartAsync();

            _runTask = SaveContainerFeedAsync();
        }

        public void Wait()
        {
            _wait.WaitOne();
        }

        public void Wait(TimeSpan timeout)
        {
            _wait.WaitOne(timeout);
        }

        public Task WaitAsync(CancellationToken cancellationToken = default)
        {
            return _wait.WaitOneAsync(cancellationToken);
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            return _wait.WaitOneAsync(timeout, cancellationToken);
        }

        public async Task CancelAsync(CancellationToken cancellationToken = default)
        {
            if (_runTask is null)
                return;

            await _changeFeedProcessor.StopAsync();
            await _snapshot.DeleteAsync(cancellationToken);
            await CloseSnapshotAsync();

            _cancelSource.Cancel();

            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Suppress
            }
        }

        public async ValueTask DisposeAsync()
        {
            await CancelAsync(CancellationToken.None);
            await CloseSnapshotAsync();
        }

        private async Task SaveContainerFeedAsync()
        {
            await _snapshot.AppendDocumentsAsync(_containerReader.GetDocuments(_documentsQuery, _cancelSource.Token), _cancelSource.Token);

            await _changeFeedProcessor.StopAsync();
            await CloseSnapshotAsync();

            // TODO: Write any additional metadata about what we just captured

            _wait.Set();
        }

        private async Task CloseSnapshotAsync()
        {
            _snapshot.Close();
            await _snapshot.DisposeAsync();
        }

        private async Task OnChangesAsync(IReadOnlyCollection<dynamic> changes, CancellationToken cancellationToken)
        {
            await _snapshot.AppendDocumentsAsync(changes.Select(JObject.FromObject), cancellationToken);
        }
    }
}
