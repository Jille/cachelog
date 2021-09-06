# cachelog

[![GoDoc](https://godoc.org/github.com/Jille/cachelog?status.svg)](https://godoc.org/github.com/Jille/cachelog)

Cachelog provides a log structured cache. Put()s are not fsynced to disk actively. The advantage is that they're way faster, the downside is that you might lose cache data. If you want to ensure to never have stale data, you can first call Delete() to clear out a piece of the cache. Delete()s are fsynced to disk in separate log files, but those are tiny writes.

The intended use case is for storing a partial (but correct) cache of file data. Get/Put/Delete all take a filename and an offset to operate on. The filename isn't treated specially so can be any []byte you want. The filename is stored a log in the logs, so using 1MB filenames will use a lot of space.

Guarantees:
* If Put() succeeds and your machine/disk doesn't crash, Get() will return the latest data. If your machine/disk does crash, some Put() calls might be lost and old data will be served once it starts up again. Assuming no CPU/memory corruption, no incorrect data will ever be stored. Bit flips on disk are detected and will cause data to be discarded (through the same mechanism we detect partial log entries: they're all md5 hashed).
* If Delete() succeeds, that data will never be served again regardless of crashes.

Put() writes to disk sequentially.

Get() does random 8 byte disk writes to indicate that the blocks have been recently accessed. If you don't need expiry, set Config.Expiry to the maximum and you'll have one write per block every 2.9 years.

You can configure garbage collection through Config.MaxGarbageRatio. The default of 0.75 allows 75% of the data in the logs to be stale before garbage collection is started. Garbage collection involves copying the still relevant blocks to the latest log file (sorting and merging them while at it) and then dropping the old file. Don't set this too low, as you'll be spending lots of IOPS on copying still active data.
