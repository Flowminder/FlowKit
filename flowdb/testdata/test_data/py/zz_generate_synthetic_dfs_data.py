#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime

from itertools import chain

import os
import pandas as pd
from functools import partial
from io import StringIO
from sqlalchemy import create_engine, select, Column, Text
from sqlalchemy.ext.declarative import declarative_base
from tohu import (
    CustomGenerator,
    SeedGenerator,
    HashDigest,
    DigitString,
    Integer,
    SelectOne,
    Timestamp,
)

num_subscribers = int(os.getenv("NUM_DFS_SUBSCRIBERS", "1000"))
num_transactions_per_date = int(os.getenv("NUM_DFS_TRANSACTIONS", "3000"))
start_date = "2016-01-01"
end_date = datetime.date(2016, 1, 1) + datetime.timedelta(
    days=int(os.getenv("N_DAYS", "7"))
)
dfs_schema = "dfs"

# Note: at this stage of the postgres startup process we connect
# without specifying the host/port and password.
db_user = os.environ["POSTGRES_USER"]
db_name = os.environ["POSTGRES_DB"]
conn_str = f"postgresql://{db_user}@/{db_name}"
engine = create_engine(conn_str)

dates = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date)]


#
# Helper function
#


def export_dataframe_to_sql(df, *, table, engine, schema, if_exists="replace"):
    """
    Helper function to export the dataframe `df` to the PostgreSQL table `schema.table`.

    This writes the data to an in-memory CSV file first and then exports it using
    a raw COPY statement, which is much faster than pandas.to_sql().
    """

    if if_exists == "replace" and engine.has_table(table, schema=schema):
        # Workaround for the fact that pandas can't drop a table if other
        # tables depend on it. Thus we issue a `TRUNCATE ... CASCADE` here
        # so that we can simply append the data to the (now empty) table.
        engine.execute(f"TRUNCATE {schema}.{table} CASCADE")
        if_exists = "append"

    conn = engine.raw_connection()
    cur = conn.cursor()
    output_str = StringIO()
    df.to_csv(output_str, sep="\t", header=False, index=False)
    output_str.seek(0)

    # Create table schema if it doesn't exist yet
    df.head(0).to_sql(table, engine, schema, if_exists=if_exists)

    cur.copy_from(output_str, f"{schema}.{table}", null="", columns=df.columns)
    conn.commit()


#
# Generate subscribers
#


class SubscriberGenerator(CustomGenerator):
    msisdn = HashDigest(length=40)
    imei = HashDigest(length=40)
    imsi = HashDigest(length=40)
    tac = DigitString(length=5)


print(f"Extracting {num_subscribers} subscribers... ", flush=True, end="")
# subscribers = SubscriberGenerator().generate(num_subscribers, seed=11111)
subscribers = pd.read_sql(
    f"SELECT msisdn, imei, imsi, tac FROM events.calls group by msisdn, imei, imsi, tac LIMIT {num_subscribers}",
    engine,
)
# Add extra subscribers if necessary
subscribers = list(
    chain(
        subscribers.itertuples(index=False, name="Subscriber"),
        SubscriberGenerator().generate(num_subscribers - len(subscribers), seed=11111),
    )
)
print("Done.")


#
# Load cells info from infrastructure.cells table in flowdb
#

print(f"Extracting cells from 'infrastructure.cells' table... ", flush=True, end="")

Base = declarative_base()


class Cells(Base):
    __tablename__ = "cells"
    __table_args__ = {"schema": "infrastructure"}
    cell_id = Column("id", Text, primary_key=True)
    version = Column("version", Text, primary_key=True)
    site_id = Column(Text)


select_stmt = select([Cells.cell_id.label("cell_id"), Cells.version, Cells.site_id])
df_cells = pd.read_sql(select_stmt, engine)

# Pick only the latest version for each cell
df_cells = df_cells.loc[df_cells.groupby("cell_id")["version"].idxmax()]
df_cells = df_cells.reset_index(drop=True)

cells = list(df_cells.itertuples(index=False, name="Cell"))
print(f"Done.\nFound {len(cells)} cells.")


#
# Create list of transaction types and store them in dfs.transaction_types.
#
# Note that we insert the `transtype_id` column manually here; in a real ETL
# process this would be inserted automatically by Postgres as a serial id.
#

print(f"Writing table {dfs_schema}.transaction_types.... ", flush=True, end="")

tx_columns = ["id", "descr", "user_type_a_party", "user_type_b_party"]
df_transaction_types = pd.DataFrame(columns=tx_columns)
df_transaction_types[
    ["descr", "user_type_a_party", "user_type_b_party"]
] = pd.DataFrame(
    [
        ("Customer checks balance", "customer", "special_1"),
        ("Customer transfers to other customer", "customer", "customer"),
        ("Customer withdraws from agent", "customer", "agent"),
        ("Customer buys airtime", "customer", "special_2"),
        ("Customer pays bill", "customer", "special_3"),
        ("Agent deposits to customer", "agent", "customer"),
        ("Agent transfers to other agent", "agent", "agent"),
        ("Agent withdraws from superagent", "agent", "superagent"),
        ("Agent checks balance", "agent", "special_4"),
        ("Agent buys airtime for customer", "agent", "special_5"),
        ("Agent asks for help from superagent", "superagent", "agent"),
        ("Superagent deposits to agent", "superagent", "agent"),
        ("Superagent transfers to other superagent", "superagent", "superagent"),
        ("Superagent checks balance", "superagent", "special_6"),
    ]
)
df_transaction_types["id"] = range(1, len(df_transaction_types) + 1)
export_dataframe_to_sql(
    df_transaction_types, table="transaction_types", engine=engine, schema=dfs_schema
)

print("Done.")

#
# Generate "raw" transactions data (= pre-ETL)
#


class TransactionsGenerator(CustomGenerator):
    def __init__(self, *, date, subscribers, cells):
        self.a_party = SelectOne(subscribers)
        self.b_party = SelectOne(subscribers)
        self.cell_from = SelectOne(cells)
        self.cell_to = SelectOne(cells)

        self.amount = Integer(0, 10000)
        self.discount = Integer(0, 100)
        self.fee = Integer(0, 500)
        self.commission = Integer(0, 2000)

        self.transtype_id = SelectOne(df_transaction_types["id"])
        self.timestamp = Timestamp(date=date)


fields = dict(
    msisdn_from="a_party.msisdn",
    msisdn_to="b_party.msisdn",
    cell_from="cell_from.cell_id",
    cell_version_from="cell_from.version",
    cell_to="cell_to.cell_id",
    cell_version_to="cell_to.version",
    amount="amount",
    discount="discount",
    fee="fee",
    commission="commission",
    transtype_id="transtype_id",
    timestamp="timestamp",
)

# Temporary workaround for the fact that CustomGenerator doesn't currently support nested loops
# (i.e, loop over all the dates and generate transactions for each of them).
tx_gen_maker = partial(TransactionsGenerator, subscribers=subscribers, cells=cells)


def generate_transactions(dates, num_transactions_per_date, *, seed, progressbar=False):
    seed_generator = SeedGenerator().reset(seed)
    df_transactions = pd.concat(
        [
            tx_gen_maker(date=date)
            .generate(
                num_transactions_per_date,
                seed=next(seed_generator),
                progressbar=progressbar,
            )
            .to_df(fields=fields)
            for date in dates
        ]
    )
    return df_transactions


# Generate transactions data for the given dates. As in the case of df_subscribers, we insert the
# transaction_id manually but in a real ETL process this would be done automatically by Postgres.
print(
    f"Generating {num_transactions_per_date} transactions per day for dates between '{start_date}' and '{end_date}'... "
)
df_transactions_raw = generate_transactions(
    dates, num_transactions_per_date, seed=99999, progressbar=True
)
df_transactions_raw.insert(
    loc=0, column="id", value=range(1, len(df_transactions_raw) + 1)
)
print("Done.")


#
# Fill the dfs.subscribers table
#

print(
    f"Extracting subscriber info into {dfs_schema}.subscribers table... ",
    flush=True,
    end="",
)

# Extract the MSISDN values from the 'raw' transactions and add a unique `id` column (which is simply an increasing value).
df_subscribers = pd.DataFrame(
    {
        "msisdn": pd.concat(
            [df_transactions_raw["msisdn_from"], df_transactions_raw["msisdn_to"]]
        ).drop_duplicates()
    }
)
df_subscribers.insert(loc=0, column="id", value=range(1, len(df_subscribers) + 1))

export_dataframe_to_sql(
    df_subscribers, table="subscribers", engine=engine, schema=dfs_schema
)

print("Done.")

#
# Fill the main dfs.transactions table
#

print(f"Filling main {dfs_schema}.transactions table... ", flush=True, end="")

df_transactions_main = df_transactions_raw[
    ["id", "amount", "discount", "fee", "commission", "transtype_id", "timestamp"]
]
export_dataframe_to_sql(
    df_transactions_main, table="transactions", engine=engine, schema=dfs_schema
)

print("Done.")

#
# Fill the dfs.transactions_metadata table
#


def get_dfs_tx_metadata_for_direction(df_transactions_raw, df_subscribers, outgoing):
    """
    Extract the subscribers and cells from the raw transactions data
    and transform them into two-line format.

    Parameters
    ----------
    df_transactions_raw : pandas.DataFrame
        The 'raw' transactions data.
    df_subscribers : pandas.DataFrame
        Dataframe containing the subscriber ids and associated msisdn values.
    outgoing : bool
        Whether to extract the outgoing (a-party) or incoming (b-party)
        part of the transaction.
    """
    assert isinstance(outgoing, bool)

    direction = "from" if outgoing else "to"
    df_tx_metadata = pd.merge(
        df_transactions_raw.rename(columns={"id": "transaction_id"}),
        df_subscribers.rename(columns={"id": "subscriber_id"}),
        left_on=f"msisdn_{direction}",
        right_on="msisdn",
        how="left",
    ).rename(
        columns={
            f"cell_{direction}": "cell_id",
            f"cell_version_{direction}": "cell_version",
        }
    )[
        ["transaction_id", "subscriber_id", "cell_id", "cell_version"]
    ]

    df_tx_metadata.insert(len(df_tx_metadata.columns), "is_outgoing", outgoing)

    return df_tx_metadata


print(f"Filling {dfs_schema}.transactions_metadata table... ", flush=True, end="")

df_transactions_metadata = pd.concat(
    [
        get_dfs_tx_metadata_for_direction(
            df_transactions_raw, df_subscribers, outgoing=True
        ),
        get_dfs_tx_metadata_for_direction(
            df_transactions_raw, df_subscribers, outgoing=False
        ),
    ]
)

export_dataframe_to_sql(
    df_transactions_metadata,
    table="transactions_metadata",
    engine=engine,
    schema=dfs_schema,
)

print("Done.")
