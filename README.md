# Jocky - For controlling your horses


### Current Idea
The idea is essentially to have a custom directory that writes immutable segments, this means only `2` file descriptors (one for writing and one for reading) are required per-index, which reduces the amount used in multi-tenant situations.

The writers have more stable performance since they depend less on the file cache to buffer writes and reads, the AIO implementation also leverages asynchronous background writing which makes CPU usage a bit more stable:

- 8 Threads, 2GB RAM, 2GB Swap, 5GB Produced index compressed.

- Green: AIO Writer
- Blue: MMapDirectory


![image](https://user-images.githubusercontent.com/57491488/207173717-b64bae6b-51e5-4e1b-a293-b8b16b29ec57.png)
![image](https://user-images.githubusercontent.com/57491488/207174299-d75e4215-f865-4494-bd86-e7d9b2a08a4e.png)
![image](https://user-images.githubusercontent.com/57491488/207178010-76b469f4-7044-4868-afd7-654eda6031b2.png)

*Writes are messured in negative MiB/s for some reason, but just ignore the `-`*

These unfinished segments as such can then be exported to a new file as the finished 'segment' as such, with each file re-organised to be contiguous. 

### Testing Screenshots

#### Mmap No Advise
![image](https://user-images.githubusercontent.com/57491488/207358796-bcbd2db0-f4af-4bfd-9d52-a95741d4437f.png)
![image](https://user-images.githubusercontent.com/57491488/207358856-35d7d838-1e82-44c4-a9ee-87b8a8700c6c.png)

#### Mmap MADV_SEQUENTIAL
![image](https://user-images.githubusercontent.com/57491488/207357681-ed8b16e8-9770-49be-8f2c-2b0871f57a23.png)
![image](https://user-images.githubusercontent.com/57491488/207358047-b22fb85c-ea4c-468f-8be9-8724dd6e8d2a.png)

#### Mmap MADV_RANDOM
![image](https://user-images.githubusercontent.com/57491488/207358409-650f8064-bad0-4a41-b2bb-cc66309a838f.png)
![image](https://user-images.githubusercontent.com/57491488/207358477-74185865-11ab-47d1-bce6-7631955efc6f.png)

#### Mmap No Advise
![image](https://user-images.githubusercontent.com/57491488/207358796-bcbd2db0-f4af-4bfd-9d52-a95741d4437f.png)
![image](https://user-images.githubusercontent.com/57491488/207358856-35d7d838-1e82-44c4-a9ee-87b8a8700c6c.png)

#### Mmap MADV_SEQUENTIAL
![image](https://user-images.githubusercontent.com/57491488/207357681-ed8b16e8-9770-49be-8f2c-2b0871f57a23.png)
![image](https://user-images.githubusercontent.com/57491488/207358047-b22fb85c-ea4c-468f-8be9-8724dd6e8d2a.png)

#### Mmap MADV_RANDOM
![image](https://user-images.githubusercontent.com/57491488/207358409-650f8064-bad0-4a41-b2bb-cc66309a838f.png)
![image](https://user-images.githubusercontent.com/57491488/207358477-74185865-11ab-47d1-bce6-7631955efc6f.png)
