# The Pansync file format

The Pansynchro framework stores configuration data for jobs, connectors, and other capabilities in `.pansync` files.  Pansync is a human-readable, text-based data format.  The aim of this document is to describe the Pansync format.

## Blocks

A Pansync file is organized in indentation-based blocks, under the block style rules of the popular Python programming language:
* Every line is indented to a depth of 0 or more.
* A colon at the end of a line, followed by the next line being indented to a greater depth level than the current line, indicates the start of a new block nested inside of the current block.
* A block ends when the next line is indented to a lesser depth than the current block's indentation, or at the end of the file.
* All lines from the one following the colon that starts a block until (but not including) the lesser-indented line that ends the block are considered part of the block.
* Nested blocks do not necessarily have to end one-at-a-time. (For example, if a block with an indentation depth of 2 is nested inside a block with an indentataion depth of 1, and a block with an indentation depth of 3 is nested inside of that block, a line after the end of the depth 3 block that is indented to a depth of 1 will end both the depth 2 block and the depth 3 block.)
* Blocks can be indented with either tabs or spaces.  (Mixing tab indentation and space indentation in the same file is treated as an error condition.)  Pansynchro's tooling will always generate files indented with one tab per indentation level; it is recommended to always follow the same convention for the sake of consistency and clarity.

## Identifiers

An *identifier* is a series of characters unbroken by a space or punctuation that is treated by Pansync as a name.  A valid identifier can be composed of letters, digits, and the underscore character, and must not begin with a digit.  So `Year2021` is a valid identifier, but `Year 2021` is not (contains a space) and neither is `2021Year` (begins with a number).

## Strings

A *string* is a block of data meant to be read as a piece of text.  A string in Pansync is denoted by opening with either a `'` (single quote) or a `"` (double quote), writing out the text, and closing with the same quote mark that opened the string.  If the string is meant to contain a single- or double- quote mark, the other quote type can be used to set off the string.  If a string needs to contain *both* types of punctuation, use double-quotes and place a `\` (backslash) in front of double-quote marks inside the string.
* `"I think he'll be happy to see you here."`
* `'He saw me and said "hey, good to see you here!"'`
* `"He saw me and said, \"hey, it's good to see you here!\""`

## Statements

Each line in a Pansync file is a *statement.*  A statement begins with an identifier providing the name of the statement, and may optionally be followed by one or more values that provide data about the statement, or by an indented block that provides more in-depth information.  For example, describing a data connection in a Job file looks like this:

```
Job:
	Connector MyConnection:
		Type Postgres
		Credentials "Connection String Goes Here"
```

At the top of the file is a `Job` statement, followed by an indented block.  The first statement in the block is a `Connector` statement, with an identifier (`MyConnection`) giving a name for the connection, to be referred to later in the file.  The `Connector` statement has its own indented block where the type of data to connect to (a Postgres database) and the credentials (a Postgres connection string) are specified.

## Value lists

Some statements have more than one value after their name. If more than one value exists, they should be separated by commas, like so:
```
supports Analyzer, Reader, Writer
```
(The space after the comma is not strictly necessary, but is included for better readability.)

## AS-values

Some commands will load in data from a source and produce new data that is meant to be used later on in the file.  These use the word `as` to specify the processing.  `as` is considered reserved and is not a valid Pansync identifier.  For example the `Analyze` command which inspects a data connection and produces a data dictionary from it, looks like this:
```
	Analyze MyConnection as MyConnection_Data
```
It operates on the `MyConnection` connector described above to produce a data dictionary named `MyConnection_Data`.

Some statements can operate on multiple values to produce multiple outputs, such as the `Harmonize` command, which checks two data dictionaries and outputs a pair that are fully compatible with one another:
```
Harmonize srcDict, destDict as srcHarmonized, destHarmonized
```

## Conclusion

This brief overview explains the high-level principles of the layout of `.pansync` data files.  Specific file types will be documented in further detail as time goes on.
