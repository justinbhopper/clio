using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotReader : IDisposable
    {
        Task ReceiveDocumentsAsync(ITargetBlock<JObject> queue, CancellationToken cancellationToken);

        void Close()
        {
            // No op by default
        }
    }
}
