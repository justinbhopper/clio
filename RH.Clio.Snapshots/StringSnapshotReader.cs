using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public abstract class StringSnapshotReader : ISnapshotReader
    {
        public async IAsyncEnumerator<JObject> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            var enumerator = GetDocumentsAsync(cancellationToken);

            try
            {
                while (await enumerator.MoveNextAsync())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    yield return JObject.Parse(enumerator.Current);
                }
            }
            finally
            {
                await enumerator.DisposeAsync();
            }
        }

        protected abstract IAsyncEnumerator<string> GetDocumentsAsync(CancellationToken cancellationToken = default);

        public virtual void Close()
        {
            // No op by default
        }
    }
}
