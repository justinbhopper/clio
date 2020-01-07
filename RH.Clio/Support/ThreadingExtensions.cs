using System.Threading.Tasks;

namespace System.Threading
{
    public static class ThreadingExtensions
    {
        public static async Task<bool> WaitOneAsync(this WaitHandle handle, int millisecondsTimeout, CancellationToken cancellationToken)
        {
            RegisteredWaitHandle? registeredHandle = null;
            var tokenRegistration = default(CancellationTokenRegistration);
            try
            {
                var tcs = new TaskCompletionSource<bool>();

                registeredHandle = ThreadPool.RegisterWaitForSingleObject(
                    handle,
                    (state, timedOut) =>
                    {
                        if (state is TaskCompletionSource<bool> source)
                        {
                            source.TrySetResult(!timedOut);
                        }
                    },
                    tcs,
                    millisecondsTimeout,
                    true);

                tokenRegistration = cancellationToken.Register(
                    state =>
                    {
                        if (state is TaskCompletionSource<bool> source)
                        {
                            source.TrySetCanceled();
                        }
                    },
                    tcs);

                return await tcs.Task;
            }
            finally
            {
                if (registeredHandle != null)
                    registeredHandle.Unregister(null);
                tokenRegistration.Dispose();
            }
        }

        public static Task<bool> WaitOneAsync(this WaitHandle handle, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return handle.WaitOneAsync((int)timeout.TotalMilliseconds, cancellationToken);
        }

        public static Task<bool> WaitOneAsync(this WaitHandle handle, CancellationToken cancellationToken)
        {
            return handle.WaitOneAsync(Timeout.Infinite, cancellationToken);
        }
    }
}
