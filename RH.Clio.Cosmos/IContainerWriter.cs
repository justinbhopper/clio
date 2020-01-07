using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public interface IContainerWriter
    {
        Task RestoreAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken = default);
    }
}
