using System;

namespace RH.Clio.Cosmos
{
    public class DocumentEventArgs : EventArgs
    {
        public DocumentEventArgs(string correlationId)
        {
            CorrelationId = correlationId;
        }

        public string CorrelationId { get; }
    }
}
