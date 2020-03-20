using System;
using Microsoft.Azure.Cosmos;

namespace RH.Clio
{
    public class CosmosClientFactory
    {
        private readonly string _host;
        private readonly string _authKey;

        public CosmosClientFactory(string host, string authKey)
        {
            _host = host;
            _authKey = authKey;
        }

        public CosmosClient CreateClient(bool allowBulkExecution)
        {
            return new CosmosClient(_host, _authKey, new CosmosClientOptions
            {
                AllowBulkExecution = allowBulkExecution,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
                MaxRetryAttemptsOnRateLimitedRequests = 0
            });
        }
    }
}
