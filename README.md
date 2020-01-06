# jepsen.etcd

Jepsen tests for the etcd consensus system! To run these, you'll need a [Jepsen
cluster](https://github.com/jepsen-io/jepsen). Then, in the standard tests,
run:

## Usage

To run a full test suite, run

```sh
lein run test-all --concurrency 2n
```

You can focus in on particular combinations of faults, or particular workloads:

```sh
lein run test-all --concurrency 2n --workload lock
lein run test-all --concurrency 2n --nemesis kill,partition
```

You can also run particular workloads and nemeses with `lein run test`. For instance, to demonstrate lost updates to a set protected by an etcd lock, try:

```sh
lein run test --workload lock-set --nemesis pause --time-limit 120
```

## License

Copyright © 2019, 2020 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
