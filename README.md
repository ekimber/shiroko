# shiroko [![Build Status](https://travis-ci.org/ekimber/shiroko.svg?branch=master)](https://travis-ci.org/ekimber/shiroko)
Clojure Data Prevalence library. 


## Installation

TODO

## Usage

Create a ref to be persisted.

  (def persistent (ref 0))

Create a transaction that modifies the ref.

  (defn add-to-x [a]
    (ref-set x (+ x a)))

Initialise the persistent store with a list of refs (these will be used to make snapshots).  Then
use `apply-transaction` to make changes to the refs.  It returns an async channel that will contain
the result of the transaction.

  (let [base (init-db :ref-list [a] :batch-size 4)]
    (dotimes [n 10]
      (println (<!! (apply-transaction inc-x)))))

The transactions will be persisted and replayed next time `init-db` is called.

Pass the returned persistent system to `take-snapshot` to take a snapshot of the refs.  The
latest snapshot is loaded at the next initialisation. This will improve start-up time when there
are a large number of transactions.

      (take-snapshot base)

## Options

FIXME: listing of options this app accepts.

## Examples

### Bugs

## License

Copyright © 2014 Edward Kimber

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
