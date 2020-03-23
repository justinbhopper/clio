using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotWriter : IAsyncDisposable
    {
        Task AppendDocumentsAsync(IReceivableSourceBlock<JObject> documents, CancellationToken cancellationToken);

        Task AppendDocumentsAsync(IEnumerable<JObject> documents, CancellationToken cancellationToken);

        void Close()
        {
            // No op by default
        }
    }
}
