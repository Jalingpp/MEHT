# MEST: An Efficient Authenticated Secondary Index for the State Data in Blockchain Systems

This is a demo of a new storage engine for verifiable non-primary key queries in blockchain systems. The primary index is Merkle Patricia Tree (MPT). The secondary indexes include MPT, Merkle Bucket Tree (MBT), and Merkle Extendible Hash Table (MEST, proposal).

## Download and Test Run

Step 1. Download the source code.

`git clone https://github.com/Jalingpp/MEST.git`

Step 2. Run test.sh to test insert and query.

`./test.sh`

Step 3. Find results in data.

## Documentation Explains

`sedb`: a folder for the storage engine that encapsulate three solutions together.

`meht`: a folder for the implementation of Merkle Extendible Hash Table (MEST), consist of `SEH.go`, `MEHT.go`, etc. components.

`mpt`: a folder for the implementation of Merkle Patricia Trie (MPT).

`mbt`: a folder for the implementation of Merkle Bucket Tree (MBT).

`mht`: a folder for the implementation of Merkle Hash Tree (mht), which is used in meht.

`util`: provides some commonly used methods, such as data reading and writing, format conversion, etc.

`test_xxx.go`: entry files used to test system performance.

## Experiments Script

We conduct experiments by the bash scripts. All the parameters with different values are listed in the bash files. You can change paramenters to test the methods.

| Script Name | Description | Parameters |
| ------ | ------ | ------ |
| run.sh | complete all experiments at once | |
| test_thread.sh | experiments over various threads | thread_option=(1 2 4 8 16 32) |

`run.sh`: complete all experiments at once. 

- `test_thread.sh`: experiments over various threads.

- `test_scale.sh`: experiments over various data scales.

- `test_cache.sh`: experiments before-after hotness adjustment.

- `test_real.sh`: experiments on NFTTrans dataset.

- `test_batchSize.sh`: experiments over various batchsize.

- `test_mehtBC.sh`: experiments over various BC of meht.

- `test_mehtBS.sh`: experiments over various BS of meht.

- `test_mbtBN.sh`: experiments over various BN of mbt.

- `test_bsfg.sh`: experiments over BSFG parameters.

An end-to-end experiment can be found in `https://github.com/Jalingpp/Geth4MEST`.