# Pushparse

## An example streaming data extractor for the compressed Pushshift Reddit database.

~2TB of data was obtained from [the Pushshift Archive](https://the-eye.eu/redarcs/).
(Remember to be a good netizen and use the Torrent option to avoid overloading the archive's servers)

With a moderately sized setup, this code can process the entire ~2TB (compressed) dataset in under an hour.

The machine used for this test:

- Dell T610
    - 2x Intel Xeon X5680 @ 3.33GHz
    - 12 cores total, 24 threads
    - 96GB RAM
    - 24TB RAID1 Array

You could achieve a faster time by using a system with more resources. Contrary to what you may think, the main limit is the RAM. Even though we are stream decompressing, each worker thread can take up to 6GB of RAM on the larger files. This is an issue with the underlying decompression library. Attempts have been made to reduce the memory footprint everywhere else in the application by using `CompactString`s.

For the system above, 8 tokio worker threads are used.

Some performace improvements are thanks to [mat](https://github.com/mat-1)
