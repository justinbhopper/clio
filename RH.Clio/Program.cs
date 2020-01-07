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

            var databaseName = "Practice-0000000000";
            var sourceContainerName = "Resources";
            var destinationContainerName = "Restored";

            //await BackupAsync(databaseName, sourceContainerName, true, loggerFactory);
            await BackupAsync(databaseName, sourceContainerName, false, loggerFactory);
            await RestoreAsync(databaseName, sourceContainerName, destinationContainerName, true, loggerFactory);
            //await RestoreAsync(databaseName, sourceContainerName, destinationContainerName, false, loggerFactory);
        }

        private static async Task BackupAsync(
            string databaseName,
            string sourceContainerName,
            bool concurrent,
            ILoggerFactory loggerFactory)
        {
            var logger = loggerFactory.CreateLogger<Program>();

            var cosmosClient = CreateClient(concurrent);
            var database = cosmosClient.GetDatabase(databaseName);
            var leaseContainerName = "Resources-lease";

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

                using var snapshotStream = File.Open(@"f:\snapshot.json", FileMode.Truncate, FileAccess.Write, FileShare.Read);
                using var changeFeedStream = File.Open(@"f:\changefeed.json", FileMode.Truncate, FileAccess.Write, FileShare.Read);

                var snapshot = new FileSnapshotWriter(snapshotStream, changeFeedStream, Encoding.UTF8);

                var query = new QueryDefinition("select * from root r where r.h = false and r.isSystem = false");
                var containerReader = new ContainerReader(sourceContainer);
                //var testReader = new TestContainerReader(containerReader, sourceContainer);
                var processor = new SnapshotProcessor(sourceContainer, leaseContainer, snapshot, query, containerReader);

                await processor.StartAsync();

                await processor.WaitAsync();

                logger.LogInformation("Snapshot took {timeMs}ms to complete ({concurrent}).", stopwatch.ElapsedMilliseconds, concurrent ? "concurrent" : "sequence");
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
            bool concurrent,
            ILoggerFactory loggerFactory)
        {
            var cosmosClient = CreateClient(concurrent);
            var database = cosmosClient.GetDatabase(databaseName);

            await database.GetContainer(destinationContainerName).DeleteContainerIfExistsAsync();

            var sourceContainer = database.GetContainer(sourceContainerName);
            var sourceContainerDetails = await sourceContainer.ReadContainerAsync();
            var destinationContainerProperties = new ContainerProperties(destinationContainerName, sourceContainerDetails.Resource.PartitionKeyPath);
            var destinationContainer = await database.CreateContainerIfNotExistsAsync(destinationContainerProperties);
            var containerWriter = new ContainerWriter(destinationContainer, loggerFactory.CreateLogger<ContainerWriter>());

            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogInformation("Restoring snapshot...");

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            using (var snapshotStream = File.Open(@"f:\snapshot.json", FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var changeFeedStream = File.Open(@"f:\changefeed.json", FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using var snapshotStreamReader = new StreamReader(snapshotStream, Encoding.UTF8);
                using var changeFeedStreamReader = new StreamReader(changeFeedStream, Encoding.UTF8);
                var snapshotReader = new StreamSnapshotReader(snapshotStreamReader, changeFeedStreamReader);

                await containerWriter.RestoreAsync(snapshotReader, concurrent);
            }

            logger.LogInformation("Restore took {timeMs}ms to complete ({concurrent}).", stopwatch.ElapsedMilliseconds, concurrent ? "concurrent" : "sequence");
        }

        private static CosmosClient CreateClient(bool allowBulkExecution)
        {
            var host = "https://localhost:8081";
            var authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            return new CosmosClient(host, authKey, new CosmosClientOptions
            {
                AllowBulkExecution = allowBulkExecution,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
                MaxRetryAttemptsOnRateLimitedRequests = 3
            });
        }
    }
}
