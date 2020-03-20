using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RH.Clio.Commands;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots;
using RH.Clio.Snapshots.Blobs;
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
        private const string s_blobAccountName = "rhapollodevelopment";
        private const string s_blobKey = "oxrOM83h3f8OUu3AmhbtCxX+/ykSS+SJgua6+8IccLoWxxwNyIV2MgQMEM7+hBNyQ/6XQtjTcbOPMz0ywawEWA==";
        private const string s_blobEndpoint = "core.windows.net";

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
                .AddTransient<CosmosClientFactory>(sp => new CosmosClientFactory(s_host, s_authKey))
                .AddTransient<BackupHandler>()
                .AddTransient<RestoreHandler>()
                .BuildServiceProvider();

            var loggerFactory = services.GetRequiredService<ILoggerFactory>();

            var databaseName = "TestDb";
            var sourceContainerName = "Conditions";
            var destinationContainerName = "Restored";
            var query = new QueryDefinition("select * from root r where r.IsHistory = false");

            var fileSnapshotFactory = new FileSnapshotFactory(s_snapshotPath, s_changefeedPath);

            //var blobContainer = new CloudBlobContainer(new Uri(s_blobEndpoint), new StorageCredentials(s_blobAccountName, s_blobKey));
            //var blobSnapshotFactory = new BlobSnapshotFactory(blobContainer, databaseName, sourceContainerName);

            ISnapshotFactory snapshotFactory = fileSnapshotFactory;

            var logger = loggerFactory.CreateLogger<Program>();
            var stopwatch = new Stopwatch();

            var sourceContainerDetails = await services.GetRequiredService<CosmosClientFactory>()
                .CreateClient(false)
                .GetDatabase(databaseName)
                .GetContainer(sourceContainerName)
                .ReadContainerAsync();

            var restoreConfig = new ContainerConfiguration(destinationContainerName, sourceContainerDetails.Resource.PartitionKeyPath, 400);

            stopwatch.Start();
            logger.LogInformation("Taking snapshot...");

            await using (var snapshot = snapshotFactory.CreateWriter())
            {
                var request = new BackupRequest(databaseName, sourceContainerName, snapshot, query);
                await services.GetRequiredService<BackupHandler>().Handle(request, CancellationToken.None);
            }

            logger.LogInformation("Snapshot took {timeMs}ms to complete.", stopwatch.ElapsedMilliseconds);

            stopwatch.Restart();
            logger.LogInformation("Restoring snapshot ({throughput} throughput)...", restoreConfig.Throughput);

            using (var snapshot = snapshotFactory.CreateReader())
            {
                var request = new RestoreRequest(databaseName, restoreConfig, snapshot)
                {
                    DropContainerIfExists = true
                };

                var documentWriteLogger = new DocumentWriteLogger(Console.CursorTop);
                request.DocumentInserted += (s, e) => documentWriteLogger.OnDocumentInserted(e.CorrelationId);
                request.DocumentInserting += (s, e) => documentWriteLogger.OnDocumentInserting();
                request.DocumentQueued += (s, e) => documentWriteLogger.OnDocumentQueued(e.CorrelationId);
                request.ThrottleWaitStarted += (s, e) => documentWriteLogger.OnThrottleStarted();
                request.ThrottleWaitFinished += (s, e) => documentWriteLogger.OnThrottleFinished();

                await services.GetRequiredService<RestoreHandler>().Handle(request, CancellationToken.None);
            }

            Console.WriteLine();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
