using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace RH.Clio.Cosmos
{
    public class ContainerWriterFactory : IContainerWriterFactory
    {
        private readonly CosmosClient _cosmosClient;
        private readonly ILoggerFactory _loggerFactory;

        public ContainerWriterFactory(
            ICosmosClientFactory clientFactory,
            ILoggerFactory loggerFactory)
        {
            _cosmosClient = clientFactory.CreateClient(false);
            _loggerFactory = loggerFactory;
        }

        public async Task<IContainerWriter> CreateWriterAsync(ContainerConfiguration config, bool dropContainerIfExists, CancellationToken cancellationToken)
        {
            var database = _cosmosClient.GetDatabase(config.DatabaseName);

            if (dropContainerIfExists)
                await database.GetContainer(config.ContainerName).DeleteContainerIfExistsAsync(cancellationToken);

            var containerProperties = new ContainerProperties(config.ContainerName, config.PartitionKeyPath);
            var container = await database.CreateContainerAsync(containerProperties, config.Throughput, cancellationToken: cancellationToken);

            return new ConcurrentContainerWriter(container, _loggerFactory.CreateLogger<ConcurrentContainerWriter>());
        }
    }
}
