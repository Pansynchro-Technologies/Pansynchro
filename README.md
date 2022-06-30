<img src="https://raw.githubusercontent.com/Pansynchro-Technologies/Pansynchro/main/Logo.png" width="256" height="256" />

# The fastest, most cost-effective data integration solution
Pansynchro is a modern, open-source, high-efficiency data transfer and processing framework for data integration and synchronization at any scale.  Heavily optimized with domain-specific algorithms, Pansynchro is designed from the ground up to minimize the time and network bandwidth, and therefore money, spent on data integration tasks.  Pansynchro is designed to make your data integration workflow as simple and efficient as possible, whether you're looking at basic API ingestion or data replication for analysis, ETL or ELT, local data or a few terabytes in the cloud.

The Pansynchro toolkit is made up of three major components: the core, connectors and data sources, and networking components.

## Core
The core, found in the `Pansynchro.Core` project, contains definitions of basic concepts that are used throughout the project.  Virtually every other piece of Pansynchro will have a dependency on `Pansynchro.Core`.

## Connectors and Data Sources
Connectors are used by Pansynchro to read and write data from/to sources such as databases, APIs and files, converting data between their specific formats and Pansynchro's internal representation.  Pansynchro supports all major SQL databases, as well as various generic data formats such as JSON and CSV.

The concept of a Data Source was developed to simplify the task of connecting to external data.  There are far more external data sources than there are data formats; it's quite common for both web service APIs and non-SQL databases to format all of their data in JSON, for example.  By creating a generic connector for JSON data, all of the details of dealing with the JSON format can be centralized, and specific code to deal with the API or database can be moved to a simpler piece of code called a Data Source which can focus on the specifics of retrieving the data, then sending it to the JSON connector for processing.

## Networking
This is perhaps Pansynchro's greatest innovation.  Data integration almost always involves data being sent across the Internet.  Typically there will be a cloud service at one or both ends of this transmission, which incurs monetary costs for both processing time and the size of data ingress/egress.  Pansynchro's networking subsystems have been built from the ground up to minimize these costs.

Competing systems typically send text across the wire, often using raw SQL `INSERT` statements for database loading and JSON-based data transmission protocols.  And while text is wonderful for the specific purpose of transmitting small-to-moderate amounts of data in a convenient, human-readable way, it also tends to be quite bulky.  When moving around gigabytes or terabytes of data that is unlikely to be directly inspected by human beings anyway, this is a suboptimal design choice to say the least.   When you're paying for data transfer by volume, the last thing you want is a high-overhead protocol that inflates that volume, often by significant percentages!

Pansynchro, by contrast, is designed to use high-efficiency data transfer whenever possible.  Database loading is done via databases' specialized bulk-upload mechanisms whenever they are available, and its network protocol is an extremely low-overhead system that starts with a highly efficient binary data representation, applies several domain-specific filtering mechanisms to reduce the data size in various cases, then applies a compression algorithm to further reduce the on-the-wire payload, thereby minimizing users' data costs.

# Getting Started

The connectors are the entry point to the Pansynchro data pipeline.  Each connector type contains a class derived from `ConnectorCore` that can be used as a factory to obtain the connectors' functionality.  They can be instantiated directly, or retrieved from `Pansynchro.Core.Connectors.ConnectorRegistry`.

The first thing to do with a connector core is to call `GetConfig` to obtain a configurator.  Use its properties to configure a connection to your data source or destination.  The configurator's `ConnectionString` property will provide the config string needed to obtain the other classes.

Once you have a config string, you will want an analyzer from the `GetAnalyzer` method.  This has an `AnalyzeAsync` method that will inspect the data and determine its structure, producing a `DataDictionary` describing the data.  The `name` parameter can be whatever arbitrary name you prefer to describe this data source or destination.  If the source and destination do not share an identical schema, the dictionaries will need to be harmonized before they can be used together.  The `DataDictionaryComparer` class can `Harmonize()` the two dictionaries, automatically adjusting minor differences and returning a `HarmonizedDictionary` containing valid `Source` and `Dest` data dicts.  If it finds differences it can't harmonize, it will put them in the `Errors` property; make sure to check this!  Differences that it can fix will be handled by the `Transformer` property; if no adjustments are needed then `Transformer` will be `null`.

With a matching `DataDictionary` in hand for both the source and the destination, data can be transferred.  From the connector core(s), use `GetReader` to obtain a reader connector and `GetWriter` to obtain a writer connector.  The data transfer can be run like so:
`await writer.Sync(reader.ReadFrom(srcDict), dstDict);`
or, if you have a transformer:
`await writer.Sync(transformer.Transform(reader.ReadFrom(srcDict)), dstDict);`

## Remote sync

To send data across the Internet, use `Pansynchro.Protocol.dll`.  The `BinaryEncoder` and `BinaryDecoder` classes can be treated as a standard writer and reader, respectively, translating between Pansynchro's internal data streams and an efficient binary format to send and receive data across the network, and can even accept TCP [clients](https://docs.microsoft.com/en-us/dotnet/api/system.net.sockets.tcpclient?view=net-6.0) and [listeners](https://docs.microsoft.com/en-us/dotnet/api/system.net.sockets.tcplistener?view=net-6.0) in their constructors to simplify the connection process.  (If you need something more sophisticated, you can do your own networking setup and use the Stream constructors instead.)

# Build Status

| Build | Status | Current Version |
| ------ | ------ | ------ |
| Packages | [![CI](https://github.com/Pansynchro-Technologies/Pansynchro/actions/workflows/dotnet.yml/badge.svg)](https://github.com/Pansynchro-Technologies/Pansynchro/actions/workflows/dotnet.yml) | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Core)](https://www.nuget.org/profiles/pansynchro) |

## Installation

[Pansynchro](https://www.nuget.org/profiles/pansynchro) components are available on NuGet. Each package is detailed below.

| Package | Current | Preview | Downloads |
| ------ | ------ | ------ | ------ |
| Pansynchro Core | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Core)](https://www.nuget.org/packages/Pansynchro.Core/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Core.svg)](https://www.nuget.org/packages/Pansynchro.Core/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Core.svg)](https://www.nuget.org/packages/Pansynchro.Core/) |
| Pansynchro SQL Core | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.SQL)](https://www.nuget.org/packages/Pansynchro.SQL/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.SQL.svg)](https://www.nuget.org/packages/Pansynchro.SQL/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.SQL.svg)](https://www.nuget.org/packages/Pansynchro.SQL/) |
| Pansynchro Network Protocol | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Protocol)](https://www.nuget.org/packages/Pansynchro.Protocol/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Protocol.svg)](https://www.nuget.org/packages/Pansynchro.Protocol/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Protocol.svg)](https://www.nuget.org/packages/Pansynchro.Protocol/) |
| Firebird Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.Firebird)](https://www.nuget.org/packages/Pansynchro.Connectors.Firebird/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.Firebird.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Firebird/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.Firebird.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Firebird/) |
| MS SQL Server Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.MSSQL)](https://www.nuget.org/packages/Pansynchro.Connectors.MSSQL/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.MSSQL.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.MSSQL/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.MSSQL.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.MSSQL/) |
| MySql Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.MySql)](https://www.nuget.org/packages/Pansynchro.Connectors.MySql/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.MySql.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.MySql/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.MySql.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.MySql/) |
| Postgres Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.Postgres)](https://www.nuget.org/packages/Pansynchro.Connectors.Postgres/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.Postgres.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Postgres/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.Postgres.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Postgres/) |
| Sqlite Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.Sqlite)](https://www.nuget.org/packages/Pansynchro.Connectors.Sqlite/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.Sqlite.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Sqlite/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.Sqlite.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Sqlite/) |
| Parquet Data Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.Parquet)](https://www.nuget.org/packages/Pansynchro.Connectors.Parquet/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.Parquet.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Parquet/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.Parquet.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Parquet/) |
| Text Data Connector | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Connectors.Textfile)](https://www.nuget.org/packages/Pansynchro.Connectors.Textfile/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Connectors.Textfile.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Textfile/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Connectors.Textfile.svg)](https://www.nuget.org/packages/Pansynchro.Connectors.Textfile/) |
| Local File Data Source | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Sources.Files)](https://www.nuget.org/packages/Pansynchro.Sources.Files/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Sources.Files.svg)](https://www.nuget.org/packages/Pansynchro.Sources.Files/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Sources.Files.svg)](https://www.nuget.org/packages/Pansynchro.Sources.Files/) |
| Web Data Source | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Sources.Http)](https://www.nuget.org/packages/Pansynchro.Sources.Http/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Sources.Http.svg)](https://www.nuget.org/packages/Pansynchro.Sources.Http/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Sources.Http.svg)](https://www.nuget.org/packages/Pansynchro.Sources.Http/) |
| S3 Data Source | [![NuGet](https://img.shields.io/nuget/v/Pansynchro.Sources.S3)](https://www.nuget.org/packages/Pansynchro.Sources.S3/) | [![Nuget](https://img.shields.io/nuget/vpre/Pansynchro.Sources.S3.svg)](https://www.nuget.org/packages/Pansynchro.Sources.S3/) | [![NuGet Downloads](https://img.shields.io/nuget/dt/Pansynchro.Sources.S3.svg)](https://www.nuget.org/packages/Pansynchro.Sources.S3/) |

# How to contribute

Pansynchro welcomes contributions from the community.  Big or small doesn't matter, as long as they're helpful!  The core code is implemented in C#, but connectors can be written in any language, their data streamed through the Pansynchro binary protocol.  (Watch this space for upcoming implementations of the binary protocol in other languages.)

# Community

[Join us on Discord](https://discord.gg/5EcjWEwgrs) to discuss the Pansynchro project and its further development.
