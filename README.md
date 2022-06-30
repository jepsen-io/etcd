# jepsen.etcd

Jepsen tests for the etcd consensus system! To run these, you'll need a [Jepsen
cluster](https://github.com/jepsen-io/jepsen).

## Quickstart

To run a full test suite, run

```sh
lein run test-all --concurrency 2n --rate 1000
```

You can focus in on particular combinations of faults, or particular workloads:

```sh
lein run test-all --concurrency 2n --workload append
lein run test-all --concurrency 2n --nemesis kill,partition
```

You can also run particular workloads and nemeses with `lein run test`. For instance, to demonstrate lost updates to a set protected by an etcd lock, try:

```sh
lein run test --workload lock-set --nemesis pause --time-limit 120
```

## Workloads

`append` appends values to keys and reads them back, using transactions to
perform multiple reads/appends atomically. Each append is implemented by
reading the current value of the key from etcd, and writing it back using a
compare guard on the observed revision, to ensure nobody has changed it during
the transaction.

`lock-set` uses an etcd lock to protect access to a shared in-memory set stored
on the local control node, inside Jepsen itself. `lock-etcd-set` does the same,
but stores the set in etcd. Both of these are unsafe, because etcd locks are
fundamentally unsafe.

`set` appends unique elements to a single key using compare-and-set transactions, and tries to read the set back.

`register` tests for linearizable reads, writes, and compare-and-set operations
on named registers. Each register is checked independently, since there are no
cross-register transactions. The checker is aware of revision information as
well as the actual value of the register.

`watch` has processes observe change events on a single key as it goes through
various revisions, then confirms that everyone observed the same state
transitions in the same order. Watch has a tendency to get stuck if we clobber
the cluster too badly, which may result in indefinite results.

## Nemeses

`pause`, `kill`, `clock`, and `partition` are the usual Jepsen nemeses for pausing/killing etcd processes, adjusting and strobing the clock, and partitioning the network. `member` adds and removes members dynamically. `bitflip-wal` and `bitflip-snap` induce single-bit errors in the WAL and snapshot files on disk.

## Options

`--concurrency 4n` says "run 4 worker threads per node".

`--lazyfs` mounts the etcd data directory in a lazyfs FUSE filesystem, and alters the `kill` nemesis to also lose un-fsynced writes.

`--ops-per-key` controls how many operations we perform on a single key.

`--nemesis FAULTS` takes a comma-separated list of faults to inject. `--nemesis-interval SECONDS` sets the approximate time between nemesis operations.

`--rate HZ` sets the rough number of operations per second Jepsen tries to perform.

`--serializable` allows etcd to serve stale data.

`--snapshot-count N` controls the snapshot count parameter to etcd; we lower this to 100 by default to stress the snapshot mechanism.

`--tcpdump` captures client traffic to each node in a pcap file.

`--unsafe-no-fsync` tells etcd to not perform fsyncs. This should be unsafe.

`--time-limit SECONDS` controls how long to run the test for.

## License

Copyright © 2019, 2020, 2022 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
