namespace RH.Clio.Configuration
{
    public class ClioConfig
    {
        public const string Section = "clio";

        public ClioConfig(string host, string authKey)
        {
            Host = host;
            AuthKey = authKey;
        }

        public string Host { get; }

        public string AuthKey { get; }
    }
}
