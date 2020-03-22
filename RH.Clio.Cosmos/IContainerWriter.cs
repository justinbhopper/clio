using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public interface IContainerWriter
    {
        Task RestoreAsync(IReceivableSourceBlock<JObject> documentsSource, CancellationToken cancellationToken = default);
    }
}
