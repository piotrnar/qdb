qdb
===

64-bit-unique-key persistent storage database

===
qdb.0, qdb.1 file contans raw records of:
 * KeySize bytes - Key
 * 32-bits LSB - Value Length
 * Value Length bytes - Value
.. the file ends with the end merker:
 * FFFFFFFF - Marker
 * 32-bits LSB - DB version sequence (used to match the log file)
 * "FINI"

===
qdb.log file format:
32-bits LSB - DB version sequence that this log file belongs to
N records:
 * 1 byte: 0-delete, 1-add
 * KeySize bytes - Key
 * If add:
    * 32-bits LSB - Value Length
    * Value Length bytes - Value
 * crc32 of the record (including the first byte)
