using Microsoft.Azure.Cosmos;

namespace RH.Clio.Cosmos
{
    public interface ICosmosClientFactory
    {
        CosmosClient CreateClient(bool allowBulkExecution);
    }
}
