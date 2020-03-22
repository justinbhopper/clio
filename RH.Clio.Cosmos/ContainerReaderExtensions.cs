using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public static class ContainerReaderExtensions
    {
        public static async IAsyncEnumerable<JObject> GetDocuments(this IContainerReader reader, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var document in reader.GetDocuments(new QueryDefinition(string.Empty), cancellationToken))
            {
                yield return document;
            }
        }
    }
}
