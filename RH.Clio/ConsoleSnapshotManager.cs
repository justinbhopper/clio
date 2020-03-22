using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using RH.Clio.Commands;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots;

namespace RH.Clio
{
    public class ConsoleSnapshotManager
    {
        private readonly BackupHandler _backupHandler;
        private readonly RestoreHandler _restoreHandler;
        private readonly ILogger<ConsoleSnapshotManager> _logger;

        public ConsoleSnapshotManager(
            BackupHandler backupHandler,
            RestoreHandler restoreHandler,
            ILogger<ConsoleSnapshotManager> logger)
        {
            _backupHandler = backupHandler;
            _restoreHandler = restoreHandler;
            _logger = logger;
        }

        public async Task BackupAsync(string databaseName, string containerName, QueryDefinition query, ISnapshotFactory snapshotFactory)
        {
            var stopwatch = new Stopwatch();

            _logger.LogInformation("Taking snapshot...");
            stopwatch.Start();

            await using (var snapshot = await snapshotFactory.CreateWriterAsync(true, CancellationToken.None))
            {
                var request = new BackupRequest(databaseName, containerName, snapshot, query);
                await _backupHandler.Handle(request, CancellationToken.None);
            }

            _logger.LogInformation("Snapshot took {timeMs}ms to complete.", stopwatch.ElapsedMilliseconds);
        }

        public async Task RestoreAsync(string databaseName, ContainerConfiguration restoreConfig, ISnapshotFactory snapshotFactory, bool allowReplace)
        {
            var stopwatch = new Stopwatch();

            _logger.LogInformation("Restoring snapshot ({throughput} throughput)...", restoreConfig.Throughput);
            stopwatch.Start();

            using (var snapshot = snapshotFactory.CreateReader())
            {
                var request = new RestoreRequest(databaseName, restoreConfig, snapshot)
                {
                    DropContainerIfExists = allowReplace
                };

                var documentWriteLogger = new DocumentWriteLogger(Console.CursorTop);
                request.DocumentInserted += (s, e) => documentWriteLogger.OnDocumentInserted(e.CorrelationId);
                request.DocumentInserting += (s, e) => documentWriteLogger.OnDocumentInserting();
                request.DocumentQueued += (s, e) => documentWriteLogger.OnDocumentQueued(e.CorrelationId);
                request.ThrottleWaitStarted += (s, e) => documentWriteLogger.OnThrottleStarted();
                request.ThrottleWaitFinished += (s, e) => documentWriteLogger.OnThrottleFinished();

                await _restoreHandler.Handle(request, CancellationToken.None);
            }

            _logger.LogInformation("Restore took {timeMs}ms to complete.", stopwatch.ElapsedMilliseconds);
        }
    }
}
