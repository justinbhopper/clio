using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public abstract class StringSnapshotReader : ISnapshotReader
    {
        public async Task ReceiveDocumentsAsync(ITargetBlock<JObject> queue, CancellationToken cancellationToken)
        {
            var transformer = new TransformBlock<string, JObject>(JObject.Parse);
            transformer.LinkTo(queue, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });

            await ReceiveDocumentsAsync(transformer, cancellationToken);
            await Task.WhenAll(transformer.Completion, queue.Completion);
        }

        protected abstract Task ReceiveDocumentsAsync(ITargetBlock<string> queue, CancellationToken cancellationToken);

        public virtual void Dispose()
        {
            // No op by default
        }

        public virtual void Close()
        {
            // No op by default
        }
    }
}
