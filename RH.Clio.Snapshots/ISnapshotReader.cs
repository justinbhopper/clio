using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotReader : IAsyncEnumerable<JObject>
    {
        void Close()
        {
            // No op by default
        }
    }
}
