# Flink Streaming File Sink Compression Issue

## Checkpoint Config:

Interval: 1s  
Timeout: 10s  
Min Pause: 500ms  
Mode: At Least Once  

## Output

The output will be bucketed by second to `$(pwd)/text`. Each part file should
be a gzip compressed tarball.

## Problem

Even with checkpointing enabled, the compressed files seem cut short. Sometimes the in-progress
files don't finish before the job is shut down.

```shell
text/15 » file part-0-0
part-0-0: gzip compressed data


text/15 » gunzip part-0-0
gzip: part-0-0: unknown suffix -- ignored


text/15 » hd part-0-0
00000000  1f 8b 08 00 00 00 00 00  00 ff                    |..........|
0000000a
```
