# shiroko [![Build Status](https://travis-ci.org/ekimber/shiroko.svg?branch=master)](https://travis-ci.org/ekimber/shiroko)
Clojure Data Prevalence library. 

Leiningen dependencies:

     [org.clojure/clojure "1.5.1"]
     [ekimber/shiroko "0.1.1"]

## Usage

Create a ref to be persisted.

    (def x (ref 0))

Create a transaction that modifies the ref.

    (defn add-to-x [a]
      (ref-set x (+ x a)))

Initialise the persistent store with a list of refs (these will be used to make snapshots).  Then
use `transaction` to make changes to the refs.  It returns an async channel that will contain
the result of the transaction. The `batch-size` option determines how many transactions will be stored
in each journal file.

    (let [base (init-db :ref-list [x] :batch-size 1000)]
      (dotimes [n 10]
        (println (<!! (transaction base (inc-x))))))

The transactions will be persisted and replayed next time `init-db` is called.

Pass the returned persistent system to `take-snapshot` to take a snapshot of the refs.  The
latest snapshot is loaded at the next initialisation. This will improve start-up time when there
are a large number of transactions.

        (take-snapshot base)

## To Do

More sophisticated journalling strategies than batch-size: max journal size, min period etc.

Help with periodic snapshotting. Although it's already pretty easy for the user to do this themselves
by periodically executing `take-snapshot`.

Tests that show the snapshot reading and journal fast-forward is working properly.

Consider a special type of persistent ref that we can somehow use to be smarter about making sure
all the correct refs are serialized in snapshots, or by preventing the user from modifying refs that
are not included in a snapshot.

## License

Copyright © 2014 Edward Kimber

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
