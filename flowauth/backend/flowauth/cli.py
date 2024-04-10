# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
from cryptography.fernet import Fernet
from flowauth.models import add_admin, init_db, make_demodata


@click.command("get-fernet")
def make_flowauth_fernet_key():
    """
    Generate a new Fernet key for symmetric encryption of data at
    rest.
    """
    print(f'FLOWAUTH_FERNET_KEY="{Fernet.generate_key().decode()}"')


@click.command("init-db")
@click.option(
    "--force/--no-force", default=False, help="Optionally wipe any existing data first."
)
def init_db_command(force: bool) -> None:
    init_db(force)
    click.echo("Initialized the database.")


@click.command("add-admin")
@click.argument("username", envvar="ADMIN_USER")
@click.argument("password", envvar="ADMIN_PASSWORD")
def add_admin_command(username, password):
    add_admin(username, password)
    click.echo(f"Added {username} as an admin.")


@click.command("demo-data")
def demo_data():
    make_demodata()
    click.echo("Made demo data.")
