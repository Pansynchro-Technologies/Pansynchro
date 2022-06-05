using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Pansynchro.SimpleUI.Shared;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Pansynchro.SimpleUI.Server.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors]
    public class ConnectorsController : ControllerBase
    {
        private ConnectorData[] _connectors;

        public ConnectorsController()
        {
            _connectors = JsonConvert.DeserializeObject<ConnectorData[]>(@"[
  {
    ""name"": ""Firebird"",
    ""description"": ""Firebird SQL database"",
    ""reader"": true,
    ""writer"": true,
    ""config"": [
      {
        ""name"": ""Data Source"",
        ""description"": ""Location of the database server"",
        ""type"": 0
      },
      {
        ""name"": ""Port"",
        ""description"": ""Port number to connect to"",
        ""type"": 1,
        ""default"": 3050
      },
      {
        ""name"": ""Database"",
        ""description"": ""Name of the database to connect to"",
        ""type"": 0
      },
      {
        ""name"": ""User ID"",
        ""description"": ""Database login username"",
        ""type"": 0
      },
      {
        ""name"": ""Password"",
        ""description"": ""Database login password"",
        ""type"": 0
      }
    ]
  },
  {
    ""name"": ""MSSQL"",
    ""description"": ""Microsoft SQL Server"",
    ""reader"": true,
    ""writer"": true,
    ""config"": [
      {
        ""name"": ""Data Source"",
        ""description"": ""Location of the database server"",
        ""type"": 0
      },
      {
        ""name"": ""Initial Catalog"",
        ""description"": ""Name of the database to connect to"",
        ""type"": 0
      },
      {
        ""name"": ""User ID"",
        ""description"": ""Database login username"",
        ""type"": 0
      },
      {
        ""name"": ""Password"",
        ""description"": ""Database login password"",
        ""type"": 0
      },
      {
        ""name"": ""Integrated Security"",
        ""description"": ""Use Windows login credentials to access a local database"",
        ""type"": 2
      },
      {
        ""name"": ""Trust Server Certificate"",
        ""description"": ""Accept the server's certificate without verifying (WARNING: this should ONLY be used for local, test databases)"",
        ""type"": 2
      }
    ]
  },
  {
    ""name"": ""MySql"",
    ""description"": ""MySql database"",
    ""reader"": true,
    ""writer"": true,
    ""config"": [
      {
        ""name"": ""Server"",
        ""description"": ""Location of the database server"",
        ""type"": 0
      },
      {
        ""name"": ""Port"",
        ""description"": ""Port number to connect to"",
        ""type"": 1,
        ""default"": 3306
      },
      {
        ""name"": ""Database"",
        ""description"": ""Name of the database to connect to"",
        ""type"": 0
      },
      {
        ""name"": ""User ID"",
        ""description"": ""Database login username"",
        ""type"": 0
      },
      {
        ""name"": ""Password"",
        ""description"": ""Database login password"",
        ""type"": 0
      }
    ]
  },
  {
    ""name"": ""Postgres"",
    ""description"": ""PostgreSQL database"",
    ""reader"": true,
    ""writer"": true,
    ""config"": [
      {
        ""name"": ""Host"",
        ""description"": ""Location of the database server"",
        ""type"": 0
      },
      {
        ""name"": ""Port"",
        ""description"": ""Port number to connect to"",
        ""type"": 1,
        ""default"": 5432
      },
      {
        ""name"": ""Database"",
        ""description"": ""Name of the database to connect to"",
        ""type"": 0
      },
      {
        ""name"": ""Username"",
        ""description"": ""Database login username"",
        ""type"": 0
      },
      {
        ""name"": ""Password"",
        ""description"": ""Database login password"",
        ""type"": 0
      },
      {
        ""name"": ""Trust Server Certificate"",
        ""description"": ""Accept the server's certificate without verifying (WARNING: this should ONLY be used for local, test databases)"",
        ""type"": 2
      }
    ]
  },
  {
    ""name"": ""Sqlite"",
    ""description"": ""Sqlite database"",
    ""reader"": true,
    ""writer"": true,
    ""config"": [
      {
        ""name"": ""DataSource"",
        ""description"": ""Location of the database file"",
        ""type"": 0
      },
      {
        ""name"": ""Password"",
        ""description"": ""Database login password"",
        ""type"": 0
      }
    ]
  },
  {
    ""name"": ""REST"",
    ""description"": ""REST API"",
    ""reader"": true,
    ""writer"": false,
    ""config"": [
      {
        ""name"": ""URL"",
        ""description"": ""Location of the API"",
        ""type"": 0
      },
      {
        ""name"": ""File Structure"",
        ""description"": ""Is the data an array of values or a single object?"",
        ""type"": 3,
        ""options"": [""Array"", ""Object""]
      },
      {
        ""name"": ""Next"",
        ""description"": ""JSONPath query to extract a Next link"",
        ""type"": 0
      },
      {
        ""name"": ""Data Path"",
        ""description"": ""JSONPath query to extract the API payload"",
        ""type"": 0
      },
      {
        ""name"": ""Error Path"",
        ""description"": ""JSONPath query to determine if an error has occurred"",
        ""type"": 0
      },
    ]
  },
  {
    ""name"": ""CSV"",
    ""description"": ""CSV file"",
    ""reader"": true,
    ""writer"": false,
    ""config"": [
      {
        ""name"": ""File"",
        ""description"": ""Location of the file"",
        ""type"": 0
      },
      {
        ""name"": ""Uses Header"",
        ""description"": ""Is the first line of the file a header?"",
        ""type"": 2
      },
      {
        ""name"": ""Next"",
        ""description"": ""JSONPath query to extract a Next link"",
        ""type"": 0
      },
      {
        ""name"": ""Data Path"",
        ""description"": ""JSONPath query to extract the API payload"",
        ""type"": 0
      },
      {
        ""name"": ""Error Path"",
        ""description"": ""JSONPath query to determine if an error has occurred"",
        ""type"": 0
      },
    ]
  }
]")!;
        }

        [HttpGet]
        public ConnectorData[] Get() => _connectors;

        [HttpPost]
        public void Post(ConfigData value)
        {

        }
    }

    public class ConfigData
    {
        public string Name { get; set; }

        public ConfigValue[] Responses { get; set; }
    }

    public class ConfigValue
    {
        public string Name { get; set; }
        public object Value { get; set; }
    }
}
