# Philosophy

The source and the destination will generally be on different computers physically distant from one another.  This means that network traffic will be involved, which almost automatically means that network traffic will be the biggest bottleneck in a sync job.  When dealing with 10s, 100s, or 1000s of gigabytes of data, this means reduced data transfer sizes will save not only time but also money, particularly if any cloud services are involved involving data metering or ingress/egress costs.  Ideally, the Pansynchro reader will be set up physically near the data source, and the writer near the destination system.

The Pansynchro serialization format is designed to push the fewest number of bytes over the wire as possible while preserving essential data.  Unlike some ETL systems that use JSON-based network protocols, Pansynchro leverages binary formats, and makes heavy use of BER compression (aka "7-bit integers") to keep the total number of bytes to a minimum.  Additional savings can be achieved, particularly in text-heavy datasets, by running the output through a compression algorithm.

# Implementation

The data is serialized in three stages: headers, metadata, data.  The data is written, and intended to be read, in a serial, forward-only fashion.  Each section is prefixed by a byte value indicating the current section, with a 0 byte at the end.  If any of these identifiers do not show up, the stream should be regarded as corrupted.

## Headers

The first step (not yet implemented) is a short header defining the following data as a Pansynchro sync job and laying out some basic metadata, including compression information.  It will be sent uncompressed; if the header denotes a compressed stream, compressed data will begin after the header is sent.  (I have various different ideas about how to implement compression; not going to finalize any of them without some testing first.)

## Metadata

After the header is sent, a name and hash of the data dictionary for the job is transmitted.  (**IMPORTANT:** This should not be the raw data dictionary produced from analyzing the source database; it should be the end result of the harmonization process, describing a data schema that is compatible with the endpoint.)  It assumes that the client already has a copy of this dictionary, which it can load locally and compare to verify the hash.  If the name check or hash check fails, the stream is considered invalid and aborted.

## Data

After the metadata for the job has been transmitted, the final step is to send the data.  It is sent as a sequence of streams, in the same order as they were listed in the data dictionary, with each stream being a sequence of column-oriented blocks, encoded by [a `BinaryWriter` serializer.](https://docs.microsoft.com/en-us/dotnet/api/system.io.binarywriter?view=net-5.0)  To guard against possible data corruption, a CRC check is added.

1) **Block sizing.**  Every block is prefixed by a BER-encoded number, indicating the number of rows in the block.  A 0 indicates the end of the stream.  Following the row count is the byte count for the block, including 4 bytes at the end for the CRC checksum, described below.
2) **Column-oriented.**  A block of rows is read from the source data, then serialized one column at a time.  (ie. field 0 of each row is written, then field 1 of each row, and so on.)  The columnar orientation keeps like data types together in the compression algorithm's sliding window, leading to improved compression ratios.
3) **CRC check.**  A [CRC32C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check#Standards_and_common_use) hash of the row buffer data is appended to the end of the serialized block data.  Failure to verify this hash is evidence of data corruption.  (It may be possible for a malicious actor to introduce corrupt data by inducing a hash collision here; if this is found to be an issue, a future version of this protocol may include stronger hash checks.)

# Optimization

To improve efficiency, the Pansynchro protocol takes a page from PNG graphics, applying various "filters" to its data to simplify it before compressing it, which either reduce the raw payload size, make the payload more compressible, or both.

## Rarely-Changed Fields

Total data transmission can be further reduced by taking advantage of the fact that many data streams have some columns with a very narrow set of distinct values.  Pansynchro's analyzer can detect this and tell the reader to order its output by this column, to create long runs where the value remains unchanged.  Rarely-changed fields (RCFs) are treated specially by the Pansynchro protocol: within a block, their value is transmitted only once per time that its value changes, followed by a BER-encoded run-length number.  RCF values will be written out in full at the start of a new block.

The RCF analyzer logic selects fields as RCFs if the total number of distinct values is less than 1/1000 of the total number of rows, in a table with at least 100,000 rows.  So if an integer field with an average BER-encoded size of 2 bytes were to be removed from a table of 512,000 rows, this will shave 1 MB off of the total sync size.  While this may seem like a minor savings, a table can have multiple RCFs, and a database can have dozens or even hundreds of tables.  Given enough data, this will add up to significant savings.

## Domain Reduction

DateTime data is serialized as the [Ticks value](https://docs.microsoft.com/en-us/dotnet/api/system.datetime.ticks?view=net-6.0#remarks), which is a 64-bit value defined as "the number of 100-nanosecond intervals that have elapsed since 12:00:00 midnight, January 1, 0001 in the Gregorian calendar."  When dealing with modern times, this makes for some extremely large numbers whose magnitude is mostly wasted on ~2000 years of redundant time in every entry.  Domain Reduction helps with this by finding the min date value for a column and subtracting it from every date value, greatly reducing the magnitude of the Ticks numbers and thus the size of the BER-encoded output.

In our test database, adding RCF and Domain Reduction optimizations reduced the final binary size by 11%.

## Sequential ID optimization

Many database tables feature a sequential ID column.  A column of numbers, each one different, won't compress particularly well, but if they're sequential and the input stream is entirely or mostly ORDERed BY that ID, there will be long runs of ID numbers that increase monotonically, ideally by 1 each time.  Encoding such columns by storing the raw value at the start of the block, and each subsequent one thereafter as the difference between the current value and the previous value, will drop a lot of `1` bytes (and a few `2`s or `3`s) into the payload for the compressor to work on.  In our test database, this reduces the final binary size by a further 2.5% after the previous optimizations had been applied.
