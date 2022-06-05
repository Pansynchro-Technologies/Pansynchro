using System;

namespace Pansynchro.NetworkServer
{
    public record ServerConfig(
        //string IPAddress,
        string DataDict,
        string InputType,
        string ConnectionString,
        string? Certificate = null,
        string[]? Thumbprint = null)
    {
        public void Validate()
        {
            /*
            if (IPAddress == null) {
                throw new Exception("Server config is missing an IP address.");
            }*/
            if (DataDict == null) {
                throw new Exception("Server config is missing a data dictionary location.");
            }
            if (InputType == null) {
                throw new Exception("Server config is missing an input type.");
            }
            if (ConnectionString == null) {
                throw new Exception("Server config is missing a connection string");
            }
        }
    }
}
