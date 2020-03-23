using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public interface IContainerReader
    {
        Task ReadDocumentsAsync(ITargetBlock<JObject> target, QueryDefinition query, CancellationToken cancellationToken = default);
    }
}
