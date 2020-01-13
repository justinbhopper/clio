using System;

namespace RH.Clio.Cosmos
{
    public class DocumentEventArgs : EventArgs
    {
        public string CorrelationId { get; } = Guid.NewGuid().ToString();
    }
}
