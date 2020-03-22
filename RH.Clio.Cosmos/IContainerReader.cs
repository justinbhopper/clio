using System.Collections.Generic;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public interface IContainerReader
    {
        IAsyncEnumerable<JObject> GetDocuments(QueryDefinition query, CancellationToken cancellationToken = default);
    }
}
