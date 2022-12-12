# Jocky - For controlling your horses


### Current Idea
The idea is essentially to have a custom directory that writes immutable segments, this means only `2` file descriptors (one for writing and one for reading) are required per-index, which reduces the amount used in multi-tenant situations.

The writers have more stable performance since they depend less on the file cache to buffer writes and reads, the AIO implementation also leverages asynchronous background writing which makes CPU usage a bit more stable:

- 8 Threads, 2GB RAM, 2GB Swap, 5GB Produced index compressed.

- Green: AIO Writer
- Blue: MMapDirectory


![image](https://user-images.githubusercontent.com/57491488/207173717-b64bae6b-51e5-4e1b-a293-b8b16b29ec57.png)
![image](https://user-images.githubusercontent.com/57491488/207174299-d75e4215-f865-4494-bd86-e7d9b2a08a4e.png)
*Writes are messured in negative MiB/s for some reason, but just ignore the `-`*

These unfinished segments as such can then be exported to a new file as the finished 'segment' as such, with each file re-organised to be contiguous. 
