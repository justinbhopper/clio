using System;
using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio
{
    public interface ISnapshotProcessor : IAsyncDisposable
    {
        Task StartAsync(CancellationToken cancellationToken = default);

        void Wait();

        void Wait(TimeSpan timeout);

        Task WaitAsync(CancellationToken cancellationToken = default);

        Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default);

        Task CancelAsync(CancellationToken cancellationToken = default);
    }
}
