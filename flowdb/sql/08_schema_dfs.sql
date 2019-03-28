/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
DFS -------------------------------------------------------

This schema contains tables related to digital financial
services (DFS) transactions.

  - transaction_types:      contains ids for all transaction types,
                            together with a textual description and
                            the user types for a-party/b-party
  - transactions:           information associated directly with each
                            transaction, such as amount, discount, fee,
                            commission
  - subscribers:            subscribers encountered in transactions data
  - transactions_metadata:  meta-data associated with each transaction,
                            such as subscriber and cell through which the
                            transaction was routed. This table is in two-
                            line format, i.e. it contains separate rows
                            for the a-party/b-party information

-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS dfs;

    CREATE TABLE IF NOT EXISTS dfs.transaction_types(

        transtype_id      SERIAL PRIMARY KEY,
        descr             TEXT,
        user_type_a_party TEXT,
        user_type_b_party TEXT

        );

    CREATE TABLE IF NOT EXISTS dfs.transactions(

        id         BIGSERIAL PRIMARY KEY,
        amount     NUMERIC(14,4),
        discount   NUMERIC(14,4),
        fee        NUMERIC(14,4),
        commission NUMERIC(14,4),
        timestamp  TIMESTAMPTZ

        );

    CREATE TABLE IF NOT EXISTS dfs.subscribers(

        id     BIGSERIAL PRIMARY KEY,
        msisdn TEXT

        );

    CREATE TABLE IF NOT EXISTS dfs.transactions_metadata(

        transaction_id BIGINT REFERENCES dfs.transactions(id),
        subscriber_id  BIGINT REFERENCES dfs.subscribers(id),
        cell_id        TEXT NOT NULL,
        cell_version   INTEGER NOT NULL,
        is_outgoing    BOOLEAN,
        FOREIGN KEY (cell_id, cell_version) REFERENCES infrastructure.cells(id, version)

        );
