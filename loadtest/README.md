# Compare Redis and Tcached

## Setup

Used two VM (Digital Ocean: 1GB 1CPU). One of them setup YCSB client and another Redis and Tcached.

Redis settings:
Disable save the DB on disk. (comment all rows SAVE ...)
Max memory policy: volatile-ttl

YCSB parameters:
recordcount=100000
operationcount=5000000
writeallfields=true

We used 3 type of load tests:

- A (50/50 read/write)
- B (95/5 read/write)
- C (100% read)

## Load tests

Workload A:

![workload A](https://github.com/tochka/tcache/blob/master/loadtest/Screenshot%202020-03-11%20at%2011.35.35.png)

Workload B:

![workload B](https://github.com/tochka/tcache/blob/master/loadtest/Screenshot%202020-03-11%20at%2011.35.53.png)

Workload C:

![workload C](https://github.com/tochka/tcache/blob/master/loadtest/Screenshot%202020-03-11%20at%2011.36.04.png)

## System metrics

Server:

![system metrics of server](https://github.com/tochka/tcache/blob/master/loadtest/Screenshot%202020-03-11%20at%2002.34.12.png)

Overhead between Tcache and Redis 100% by RAM

Client:

![system metrics of client](https://github.com/tochka/tcache/blob/master/loadtest/Screenshot%202020-03-11%20at%2002.33.48.png)