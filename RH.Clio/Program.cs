using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots.IO;

namespace RH.Clio
{
    public class Program
    {
        public static async Task Main()
        {
            var host = "https://localhost:8081";
            var authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            var cosmosClient = new CosmosClient(host, authKey);

            var databaseName = "Practice-0000000000";
            var sourceContainerName = "Resources";
            var destinationContainerName = "Restored";
            var leaseContainerName = "Resources-lease";
            var database = cosmosClient.GetDatabase(databaseName);

            var sourceContainer = database.GetContainer(sourceContainerName);

            try
            {
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
            }
            finally
            {
                await database.GetContainer(leaseContainerName).DeleteContainerIfExistsAsync();
            }

            await database.GetContainer(destinationContainerName).DeleteContainerIfExistsAsync();

            var sourceContainerDetails = await sourceContainer.ReadContainerAsync();
            var destinationContainerProperties = new ContainerProperties(destinationContainerName, sourceContainerDetails.Resource.PartitionKeyPath);
            var destinationContainer = await database.CreateContainerIfNotExistsAsync(destinationContainerProperties);
            var containerWriter = new ContainerWriter(destinationContainer);

            using (var snapshotStream = File.Open(@"f:\snapshot.json", FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var changeFeedStream = File.Open(@"f:\changefeed.json", FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using var snapshotStreamReader = new StreamReader(snapshotStream, Encoding.UTF8);
                using var changeFeedStreamReader = new StreamReader(changeFeedStream, Encoding.UTF8);
                var snapshotReader = new StreamSnapshotReader(snapshotStreamReader, changeFeedStreamReader);

                await containerWriter.RestoreAsync(snapshotReader);
            }
        }
    }
}
