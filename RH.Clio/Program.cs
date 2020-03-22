using System;
using System.IO;
using System.Text;
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
        private const string s_host = "https://localhost:8081";
        private const string s_authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string s_blobAccountName = "rhapollodevelopment";
        private const string s_blobKey = "7TY7CABaQtLXHunAnK4xkCxN7IEf/W/gXnVx3TrFFZ5pStlIrAr2MyKrmZeVTIS6/W2wzvV/WlAafVSz4hIxPw==";
        private const string s_blobContainer = "https://rhapollodevelopment.blob.core.windows.net/backup-test";

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
                .AddTransient<ICosmosClientFactory>(sp => new CosmosClientFactory(s_host, s_authKey))
                .AddTransient<BackupHandler>()
                .AddTransient<RestoreHandler>()
                .AddTransient<ConsoleSnapshotManager>()
                .BuildServiceProvider();

            var loggerFactory = services.GetRequiredService<ILoggerFactory>();

            var databaseName = "TestDb";
            var sourceContainerName = "Conditions";
            var destinationContainerName = "Restored";
            var query = new QueryDefinition("select * from root r");

            var fileSnapshotFactory = new FileSnapshotFactory(s_snapshotPath);

            var blobContainer = new CloudBlobContainer(new Uri(s_blobContainer), new StorageCredentials(s_blobAccountName, s_blobKey));
            var blobSnapshotFactory = new BlobSnapshotFactory(blobContainer, 100);

            ISnapshotFactory snapshotFactory = blobSnapshotFactory;

            var sourceContainerDetails = await services.GetRequiredService<ICosmosClientFactory>()
                .CreateClient(false)
                .GetDatabase(databaseName)
                .GetContainer(sourceContainerName)
                .ReadContainerAsync();

            var restoreConfig = new ContainerConfiguration(destinationContainerName, sourceContainerDetails.Resource.PartitionKeyPath, 400);

            var manager = services.GetRequiredService<ConsoleSnapshotManager>();

            await manager.BackupAsync(databaseName, sourceContainerName, query, snapshotFactory);
            await manager.RestoreAsync(databaseName, restoreConfig, snapshotFactory, true);

            Console.WriteLine();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
