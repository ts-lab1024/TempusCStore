# TempusCStore

## Proof & Experiments
We provide the proof and additional experimental results (impact of item/chunk size) in Supplement.pdf.

## Dependencies
```angular2html
$ sudo apt-get install libyaml-cpp-dev cmake build-essential libboost-all-dev google-perftools libprotobuf-dev protobuf-compiler libgoogle-perftools-dev libsnappy-dev
$ git clone https://github.com/jemalloc/jemalloc.git
$ cd jemalloc
$ autoconf
$ ./configure
$ make
$ sudo make install
```

## Build
```angular2html
$ mkdir build && cd build && cmake ..
$ make TempusCStore_test
```

## Run Test
```angular2html
$ cd build
$ ./TempusCStore_test [thread_num] [timeseries_num] [sampling interval] [sample_tuple_num]
```
example: ./TempusCStore_test 32 1000000 30 90

## Configuration
refer to [config.yaml](./config.yaml) for details

Parameters:
* db_path : path for lsm-tree storage
* log_path : path for WAL
* wal_num : number of WAl thread num
* tree_series_path : path for TreeSeries storage
* tree_series_info_path : path for TreeSeries info storage
* tree_series_thread_pool_size : size of thread pool used in TreeSeries
* max_slab_memory : TreeSeries memory size
* read_buffer_size : buffer size used to read disk TreeSeries into memory
* slab_item_size : size of one single item in slab
* chunk_size : bytes of compressed data samples in one chunk
* slab_size : size of storage unit in TreeSeries
* leveldb_max_imm_num : number of Immtables in lsm-tree
* leveldb_write_buffer_size : write buffer size of lsm-tree
* leveldb_max_file_size : max file size of lsm-tree

## Baseline & Benchmark
ForestTI: https://github.com/naivewong/forestti

TimeUnion: https://github.com/naivewong/timeunion

Prometheus tsdb: https://github.com/prometheus-junkyard/tsdb

Apache IoTDB: https://github.com/apache/iotdb

InfluxDB https://github.com/influxdata/influxdb

TimescaleDB https://github.com/timescale/timescaledb

TSBS https://github.com/timescale/tsbs    

BenchAnt’s TSBS supports Apache IoTDB https://github.com/benchANT/tsbs 
