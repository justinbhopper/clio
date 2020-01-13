using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots.IO;
using Serilog;
using Serilog.Extensions.Logging;

namespace RH.Clio
{
    public class Program
    {
        private const string s_snapshotPath = @"f:\snapshot.json";
        private const string s_changefeedPath = @"f:\changefeed.json";
        private const string s_host = "https://localhost:8081";
        private const string s_authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        public static async Task Main()
        {
            var seriLogger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            var services = new ServiceCollection()
                .AddLogging(builder =>
                {
                    builder
                        .AddSerilog(dispose: true)
                        .SetMinimumLevel(LogLevel.Trace);
                })
                .AddSingleton<ILoggerFactory>(services => new SerilogLoggerFactory(seriLogger, true))
                .BuildServiceProvider();

            var loggerFactory = services.GetRequiredService<ILoggerFactory>();

            var cosmosClient = CreateClient(false);

            var databaseName = "TestDb";
            var sourceContainerName = "Conditions";
            var destinationContainerName = "Restored";
            var query = new QueryDefinition("select * from root r where r.IsHistory = false");

            await BackupAsync(databaseName, sourceContainerName, query, loggerFactory);
            await RestoreAsync(databaseName, sourceContainerName, destinationContainerName, loggerFactory);
        }

        private static async Task BackupAsync(
            string databaseName,
            string sourceContainerName,
            QueryDefinition query,
            ILoggerFactory loggerFactory)
        {
            var logger = loggerFactory.CreateLogger<Program>();

            var cosmosClient = CreateClient(false);
            var database = cosmosClient.GetDatabase(databaseName);
            var leaseContainerName = sourceContainerName + "-lease";

            var sourceContainer = database.GetContainer(sourceContainerName);

            // Should we own the lease container?  Leaving it around sometimes seems to cause issues with future runs...
            await database.GetContainer(leaseContainerName).DeleteContainerIfExistsAsync();

            var stopwatch = new Stopwatch();

            try
            {
                stopwatch.Start();

                logger.LogInformation("Taking snapshot...");

                var leaseContainerProperties = new ContainerProperties(leaseContainerName, "/id");
                var leaseContainer = await database.CreateContainerIfNotExistsAsync(leaseContainerProperties, throughput: 400);

                using var snapshotStream = File.Open(s_snapshotPath, FileMode.Create, FileAccess.Write, FileShare.Read);
                using var changeFeedStream = File.Open(s_changefeedPath, FileMode.Create, FileAccess.Write, FileShare.Read);

                var snapshot = new FileSnapshotWriter(snapshotStream, changeFeedStream, Encoding.UTF8);

                var containerReader = new ContainerReader(sourceContainer);
                //var testReader = new TestContainerReader(containerReader, sourceContainer);
                var processor = new SnapshotProcessor(sourceContainer, leaseContainer, snapshot, query, containerReader);

                await processor.StartAsync();

                await processor.WaitAsync();

                logger.LogInformation("Snapshot took {timeMs}ms to complete.", stopwatch.ElapsedMilliseconds);
            }
            finally
            {
                await database.GetContainer(leaseContainerName).DeleteContainerIfExistsAsync();
            }
        }

        private static async Task RestoreAsync(
            string databaseName,
            string sourceContainerName,
            string destinationContainerName,
            ILoggerFactory loggerFactory)
        {
            var cosmosClient = CreateClient(true);
            var database = cosmosClient.GetDatabase(databaseName);

            await database.GetContainer(destinationContainerName).DeleteContainerIfExistsAsync();

            var sourceContainer = database.GetContainer(sourceContainerName);
            var sourceContainerDetails = await sourceContainer.ReadContainerAsync();
            var destinationContainerProperties = new ContainerProperties(destinationContainerName, sourceContainerDetails.Resource.PartitionKeyPath);
            var destinationContainer = await database.CreateContainerIfNotExistsAsync(destinationContainerProperties);
            var containerWriter = new ConcurrentContainerWriter(destinationContainer, loggerFactory.CreateLogger<ConcurrentContainerWriter>());
            var documentWriteLogger = new DocumentWriteLogger(Console.CursorTop);

            containerWriter.DocumentInserted += (s, e) => documentWriteLogger.OnDocumentInserted(e.CorrelationId);
            containerWriter.DocumentInserting += (s, e) => documentWriteLogger.OnDocumentInserting();
            containerWriter.DocumentQueued += (s, e) => documentWriteLogger.OnDocumentQueued(e.CorrelationId);
            containerWriter.ThrottleWaitStarted += (s, e) => documentWriteLogger.OnThrottleStarted();
            containerWriter.ThrottleWaitFinished += (s, e) => documentWriteLogger.OnThrottleFinished();

            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogInformation("Restoring snapshot...");

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            using (var snapshotStream = File.Open(s_snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var changeFeedStream = File.Open(s_changefeedPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using var snapshotStreamReader = new StreamReader(snapshotStream, Encoding.UTF8);
                using var changeFeedStreamReader = new StreamReader(changeFeedStream, Encoding.UTF8);
                var snapshotReader = new StreamSnapshotReader(snapshotStreamReader, changeFeedStreamReader);

                await containerWriter.RestoreAsync(snapshotReader);
            }

            logger.LogInformation("Restore took {timeMs}ms to complete.", stopwatch.ElapsedMilliseconds);
        }

        private static CosmosClient CreateClient(bool allowBulkExecution)
        {
            return new CosmosClient(s_host, s_authKey, new CosmosClientOptions
            {
                AllowBulkExecution = allowBulkExecution,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
                MaxRetryAttemptsOnRateLimitedRequests = 0
            });
        }
    }
}
