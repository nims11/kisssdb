# kisssdb
(Keep it) Simple Stupid Small Database

Inspired from [kissdb](https://github.com/adamierymenko/kissdb).

KISSSDB is a simple key/value store built to deal with the limitations of kissdb, without sacrificing its simplicity. kisssdb supports variable length keys and values making it a very space efficient alternative.

Features:

- Same as kissdb
- Small memory footprint (smaller than kissdb: unlike kissdb, kisssdb doesn't keep the hash table in the memory)
- Extremely space efficient due to the support for variable length key/value


Limitations:

- Same as kissdb except variable sized keys/values
