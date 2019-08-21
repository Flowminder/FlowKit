# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Dict, Union

import datetime
import pyotp
from flowauth.invalid_usage import Unauthorized
from itertools import chain

import click
from cryptography.fernet import Fernet
from flask import current_app
from flask.cli import with_appcontext
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.hybrid import hybrid_property
from passlib.hash import argon2

db = SQLAlchemy()


# Link table for mapping users to groups
group_memberships = db.Table(
    "group_memberships",
    db.Column("user_id", db.Integer, db.ForeignKey("user.id"), primary_key=True),
    db.Column("group_id", db.Integer, db.ForeignKey("group.id"), primary_key=True),
)

# Link table for mapping spatial aggregation units to server capabilities
spatial_capabilities = db.Table(
    "spatial_capabilities",
    db.Column(
        "spatial_aggregation_unit_id",
        db.Integer,
        db.ForeignKey("spatial_aggregation_unit.id"),
        primary_key=True,
    ),
    db.Column(
        "server_capability_id",
        db.Integer,
        db.ForeignKey("server_capability.id"),
        primary_key=True,
    ),
)

# Link table for mapping spatial aggregation units to group permissions
group_spatial_capabilities = db.Table(
    "group_spatial_capabilities",
    db.Column(
        "spatial_aggregation_unit_id",
        db.Integer,
        db.ForeignKey("spatial_aggregation_unit.id"),
        primary_key=True,
    ),
    db.Column(
        "group_server_permission_id",
        db.Integer,
        db.ForeignKey("group_server_permission.id"),
        primary_key=True,
    ),
)


def get_fernet() -> Fernet:
    """
    Get the app's Fernet object to encrypt & decrypt things.
    Returns
    -------
    crypography.fernet.Fernet
    """
    return Fernet(current_app.config["FLOWAUTH_FERNET_KEY"])


class User(db.Model):
    """
    A user. Has at least one group.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(), unique=True, nullable=False)
    _password = db.Column(db.String(), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    groups = db.relationship(
        "Group",
        secondary=group_memberships,
        lazy="subquery",
        backref=db.backref("members", lazy=True),
    )
    tokens = db.relationship(
        "Token", back_populates="owner", cascade="all, delete, delete-orphan"
    )
    two_factor_auth = db.relationship(
        "TwoFactorAuth",
        back_populates="user",
        cascade="all, delete, delete-orphan",
        uselist=False,
    )
    require_two_factor = db.Column(db.Boolean, default=False)

    @property
    def two_factor_setup_required(self) -> bool:
        return (
            self.two_factor_auth is None or not self.two_factor_auth.enabled
        ) and self.require_two_factor

    def is_authenticated(self) -> bool:
        return True

    def is_active(self) -> bool:
        return True

    def is_anonymous(self) -> bool:
        return False

    def get_id(self) -> int:
        return self.id

    def is_correct_password(self, plaintext) -> bool:
        """
        Verify if a password is correct.

        Parameters
        ----------
        plaintext: str
            Input to check

        Returns
        -------
        bool

        """
        return argon2.verify(plaintext, self._password)

    def allowed_claims(self, server) -> dict:
        """
        Get the claims the user is allowed to generate tokens for on a server.

        Parameters
        ----------
        server: Server
            Server to check against

        Returns
        -------
        dict

        """

        allowed = {}
        for cap in server.capabilities:
            using_groups = cap.group_uses
            my_rights = [p for p in using_groups if p.group in self.groups]
            get_result = cap.get_result and any(p.get_result for p in my_rights)
            run = cap.run and any(p.run for p in my_rights)
            poll = cap.poll and any(p.poll for p in my_rights)
            group_agg_units = set(
                chain(*[right.spatial_aggregation for right in my_rights])
            )
            agg_units = [
                agg.name for agg in cap.spatial_aggregation if agg in group_agg_units
            ]
            if any((run, poll, get_result)):
                allowed[cap.capability.name] = {
                    "permissions": {"run": run, "poll": poll, "get_result": get_result},
                    "spatial_aggregation": agg_units,
                }
        return allowed

    def latest_token_expiry(self, server: "Server") -> datetime.datetime:
        """
        Get the latest datetime a token can be valid until on a server.

        Parameters
        ----------
        server: Server
            Server to get the user's latest expiry time on.
        Returns
        -------
        datetime.datetime
            Latest datetime this user can have a token expire at on this server

        """
        limits = self.token_limits(server)
        life = limits["longest_life"]
        end = limits["latest_end"]
        hypothetical_max = datetime.datetime.now() + datetime.timedelta(minutes=life)
        return min(end, hypothetical_max)

    def token_limits(
        self, server: "Server"
    ) -> Dict[str, Union[datetime.datetime, int]]:
        """
        Get the maximum lifetime and latest expiry date a token can be
        created for on this user on a server.

        Returns
        -------
        dict
            Dict {"latest_end": datetime, "longest_life":int}
        """
        latest, longest = zip(
            *[
                (limit.latest_end, limit.longest_life)
                for limit in server.group_token_limits
                if limit.group in self.groups
            ]
        )
        return {
            "latest_end": min(server.latest_token_expiry, max(latest)),
            "longest_life": min(server.longest_token_life, max(longest)),
        }

    @hybrid_property
    def password(self) -> str:
        """

        Notes
        -----
        When called on the class, returns the SQLAlchemy QueryableAttribute

        Returns
        -------
        str
            The encrypted password as a string when called on an instance.
        """
        return self._password

    @password.setter
    def password(self, plaintext: str):
        self._password = argon2.hash(plaintext)

    def __repr__(self) -> str:
        return f"<User {self.username}>"


class TwoFactorAuth(db.Model):
    user_id = db.Column(
        db.Integer, db.ForeignKey("user.id"), nullable=False, primary_key=True
    )
    user = db.relationship("User", back_populates="two_factor_auth", lazy=True)
    enabled = db.Column(db.Boolean, nullable=False, default=False)
    _secret_key = db.Column(db.String(), nullable=False)  # Encrypted in db
    two_factor_backups = db.relationship(
        "TwoFactorBackup", back_populates="auth", cascade="all, delete, delete-orphan"
    )

    def validate(self, code: str) -> bool:
        """
        Validate a code against the otp generator, and if that fails, the backup codes, and
        mark as just used.

        A valid code is only valid once.

        Parameters
        ----------
        code : str
            Code to check

        Returns
        -------
        bool
            True if the code is a valid OTP

        Raises
        ------
        Unauthorized
            Raised if the code is invalid, or has just been used.
        """
        current_app.logger.debug(f"Verifying {code} using {self.decrypted_secret_key}")
        is_valid = pyotp.totp.TOTP(self.decrypted_secret_key).verify(code)
        if is_valid:
            if (
                current_app.config["CACHE_BACKEND"].get(self.user_id) == code
            ):  # Reject if the code is being reused
                raise Unauthorized("Code not valid.")
            else:
                current_app.config["CACHE_BACKEND"].set(self.user_id, code)
            return True
        else:
            raise Unauthorized("Code not valid.")

    def validate_backup_code(self, plaintext: str) -> bool:
        """
        Verify if a password is correct.

        Parameters
        ----------
        plaintext: str
            Input to check

        Returns
        -------
        bool

        Raises
        ------
        Unauthorized
            If the backup code is not valid
        """
        for code in self.two_factor_backups:
            try:
                if code.verify(plaintext):
                    db.session.delete(code)
                    db.session.commit()
                    return True
            except Unauthorized:
                pass  # Need to check them all
        raise Unauthorized("Code not valid.")

    @hybrid_property
    def secret_key(self) -> str:
        """

        Notes
        -----
        When called on the class, returns the SQLAlchemy QueryableAttribute

        Returns
        -------
        str
            The encrypted secret key as a string when called on an instance.
        """
        return self._secret_key

    @property
    def decrypted_secret_key(self) -> str:
        """
        Decrypted per user otp secret.

        Returns
        -------
        str
            Returns the decrypted secret key
        """
        key = self._secret_key
        try:
            key = key.encode()
        except AttributeError:
            pass  # Already bytes
        try:
            return get_fernet().decrypt(key).decode()
        except Exception as exc:
            current_app.logger.debug(
                f"Failed to decrypt '{key}'. Original was '{self._secret_key}'. Error was {exc}"
            )
            raise exc

    @secret_key.setter
    def secret_key(self, plaintext: str):
        """
        Encrypt, then store to the database the per user otp secret.

        Parameters
        ----------
        plaintext: str
            Key to encrypt.
        """
        self._secret_key = get_fernet().encrypt(plaintext.encode()).decode()


class TwoFactorBackup(db.Model):
    """
    Back up login codes for two-factor auth.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    auth_id = db.Column(
        db.Integer, db.ForeignKey("two_factor_auth.user_id"), nullable=False
    )
    auth = db.relationship(
        "TwoFactorAuth", back_populates="two_factor_backups", lazy=True
    )
    _backup_code = db.Column(db.String(), nullable=False)

    def verify(self, plaintext: str) -> bool:
        """

        Parameters
        ----------
        plaintext : str
            Code to verify

        Returns
        -------
        bool
            True if a valid code.

        Raises
        ------
        Unauthorized
            Raised if the code is not valid

        """
        if argon2.verify(plaintext, self._backup_code):
            return True
        else:
            raise Unauthorized("Code not valid.")

    @hybrid_property
    def backup_code(self) -> str:
        """

        Notes
        -----
        When called on the class, returns the SQLAlchemy QueryableAttribute

        Returns
        -------
        str
            The encrypted backup code as a string when called on an instance.
        """
        return self._backup_code

    @backup_code.setter
    def backup_code(self, plaintext: str):
        self._backup_code = argon2.hash(plaintext)


class Token(db.Model):
    """
    An instance of a token.
    Is owned by one user, applies to one server, has an expiry time, encodes
    several capabilties for a server.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), nullable=False)
    _token = db.Column(db.Text, nullable=False)
    expires = db.Column(db.DateTime, nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    owner = db.relationship("User", back_populates="tokens", lazy=True)
    server_id = db.Column(db.Integer, db.ForeignKey("server.id"), nullable=False)
    server = db.relationship("Server", back_populates="tokens", lazy=True)

    @hybrid_property
    def token(self) -> str:
        """

        Notes
        -----
        When called on the class, returns the SQLAlchemy QueryableAttribute

        Returns
        -------
        str
            The encrypted token as a string when called on an instance.
        """
        return self._token

    @property
    def decrypted_token(self) -> str:
        """
        Decrypted token.

        Returns
        -------
        str
            Returns the decrypted token.
        """
        token = self._token
        try:
            token = token.encode()
        except AttributeError:
            pass  # Already bytes
        return get_fernet().decrypt(token).decode()

    @token.setter
    def token(self, plaintext: str):
        """
        Encrypt, then store to the database the token string.

        Parameters
        ----------
        plaintext: str
            Token to encrypt.
        """
        self._token = get_fernet().encrypt(plaintext.encode()).decode()

    def __repr__(self) -> str:
        return f"<Token {self.owner}:{self.server}>"


class Server(db.Model):
    """
    A server. Has a name, and a secret key, and upper bounds on token expiry and lifetime.
    A server has some set of available capabilities.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), unique=True, nullable=False)
    latest_token_expiry = db.Column(db.DateTime, nullable=False)
    longest_token_life = db.Column(db.Integer, nullable=False)
    tokens = db.relationship(
        "Token", back_populates="server", cascade="all, delete, delete-orphan"
    )
    capabilities = db.relationship(
        "ServerCapability",
        back_populates="server",
        cascade="all, delete, delete-orphan",
    )
    group_token_limits = db.relationship(
        "GroupServerTokenLimits",
        back_populates="server",
        cascade="all, delete, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Server {self.name}>"


class ServerCapability(db.Model):
    """
    The set of API capabilities which are available on a server, which is
    a subset of Capabilities.
    """

    id = db.Column(db.Integer, primary_key=True)
    get_result = db.Column(db.Boolean, default=False)
    run = db.Column(db.Boolean, default=False)
    poll = db.Column(db.Boolean, default=False)

    server_id = db.Column(db.Integer, db.ForeignKey("server.id"), nullable=False)
    server = db.relationship("Server", back_populates="capabilities", lazy=True)
    capability_id = db.Column(
        db.Integer, db.ForeignKey("capability.id"), nullable=False
    )
    capability = db.relationship("Capability", back_populates="usages", lazy=True)
    group_uses = db.relationship(
        "GroupServerPermission",
        back_populates="server_capability",
        lazy=True,
        cascade="all, delete, delete-orphan",
    )
    __table_args__ = (
        db.UniqueConstraint("server_id", "capability_id", name="_server_cap_uc"),
    )  # Enforce only one of each capability per server

    def __repr__(self) -> str:
        return f"<ServerCapability {self.capability}> {self.get_result}:{self.run}:{self.poll}, {self.spatial_aggregation}@{self.server}>"


class GroupServerTokenLimits(db.Model):
    """
    The maximum lifetime of tokens that a group may create for a server.
    Must be <= the maximum limits for that server.
    """

    id = db.Column(db.Integer, primary_key=True)
    latest_end = db.Column(db.DateTime, nullable=False)
    longest_life = db.Column(db.Integer, nullable=False)
    server_id = db.Column(db.Integer, db.ForeignKey("server.id"), nullable=False)
    server = db.relationship("Server", back_populates="group_token_limits", lazy=True)
    group_id = db.Column(db.Integer, db.ForeignKey("group.id"), nullable=False)
    group = db.relationship("Group", back_populates="server_token_limits", lazy=True)
    __table_args__ = (
        db.UniqueConstraint("group_id", "server_id", name="_group_server_limits_uc"),
    )  # Enforce only one per group-server combination

    def __repr__(self) -> str:
        return f"<GroupServerTokenLimits {self.group} {self.server}>"


class GroupServerPermission(db.Model):
    """
    The set of API capabilities that a group has for a server.
    Must be a subset of the available capabilities for that server.
    """

    id = db.Column(db.Integer, primary_key=True)
    get_result = db.Column(db.Boolean, default=False)
    run = db.Column(db.Boolean, default=False)
    poll = db.Column(db.Boolean, default=False)
    group_id = db.Column(db.Integer, db.ForeignKey("group.id"), nullable=False)
    group = db.relationship("Group", back_populates="server_permissions", lazy=True)
    server_capability_id = db.Column(
        db.Integer, db.ForeignKey("server_capability.id"), nullable=False
    )
    server_capability = db.relationship(
        "ServerCapability", back_populates="group_uses", lazy=True
    )
    __table_args__ = (
        db.UniqueConstraint(
            "group_id", "server_capability_id", name="_group_servercap_uc"
        ),
    )  # Enforce only only group - capability pair

    def __repr__(self) -> str:
        return f"<GroupServerPermission {self.server_capability.capability}> {self.get_result}:{self.run}:{self.poll}, {self.spatial_aggregation} {self.group}@{self.server_capability.server}>"


class SpatialAggregationUnit(db.Model):
    """
    An unit of spatial aggregation.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), unique=True, nullable=False)
    server_usages = db.relationship(
        "ServerCapability",
        secondary=spatial_capabilities,
        lazy="subquery",
        backref=db.backref("spatial_aggregation", lazy=True),
    )

    group_server_capability_usages = db.relationship(
        "GroupServerPermission",
        secondary=group_spatial_capabilities,
        lazy="subquery",
        backref=db.backref("spatial_aggregation", lazy=True),
    )

    def __repr__(self) -> str:
        return f"<SpatialAggregationUnit {self.name}>"


class Capability(db.Model):
    """
    An API capability.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), unique=True, nullable=False)
    usages = db.relationship(
        "ServerCapability",
        back_populates="capability",
        cascade="all, delete, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Capability {self.name}>"


class Group(db.Model):
    """
    A group of users. Has some set of permissions on some set of servers.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(), unique=True, nullable=False)
    user_group = db.Column(db.Boolean, default=False)
    server_token_limits = db.relationship(
        "GroupServerTokenLimits",
        back_populates="group",
        cascade="all, delete, delete-orphan",
    )
    server_permissions = db.relationship(
        "GroupServerPermission",
        back_populates="group",
        cascade="all, delete, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Group {self.name}>"


@click.command("init-db")
@click.option(
    "--force/--no-force", default=False, help="Optionally wipe any existing data first."
)
@with_appcontext
def init_db_command(force: bool) -> None:
    init_db(force)
    click.echo("Initialized the database.")


def init_db(force: bool = False) -> None:
    """
    Initialise the database, optionally wipe any existing one first.

    Parameters
    ----------
    force : bool
        If set to true, wipes any existing database.
    Returns
    -------
    None
    """
    if current_app.config["DB_IS_SET_UP"].is_set():
        current_app.logger.debug("Database already set up by another worker, skipping.")
        return
    current_app.logger.debug("Initialising db.")
    if force:
        current_app.logger.debug("Dropping existing db.")
        db.drop_all()
    db.create_all()
    current_app.config["DB_IS_SET_UP"].set()
    current_app.logger.debug("Initialised db.")


@click.command("add-admin")
@click.argument("username", envvar="ADMIN_USER")
@click.argument("password", envvar="ADMIN_PASSWORD")
@with_appcontext
def add_admin_command(username, password):
    add_admin(username, password)
    click.echo(f"Added {username} as an admin.")


def add_admin(username: str, password: str) -> None:
    """
    Add an administrator, or reset their password if they already exist.

    Parameters
    ----------
    username : str
        Username for the admin
    password : str
        Password for the admin

    Returns
    -------
    None

    """
    u = User.query.filter(User.username == username).first()
    if u is None:
        current_app.logger.debug(f"Creating new admin {username}.")
        u = User(username=username, password=password, is_admin=True)
        ug = Group(name=username, user_group=True)
        ug.members.append(u)
        db.session.add(ug)
    else:
        current_app.logger.debug(f"Promoting {username} to admin.")
        u.password = password
        u.is_admin = True

    db.session.add(u)

    db.session.commit()


def make_demodata():
    """
    Generate some demo data.
    """
    if current_app.config["DB_IS_SET_UP"].is_set():
        current_app.logger.debug("Database already set up by another worker, skipping.")
        return
    current_app.logger.debug("Creating demo data.")
    db.drop_all()
    db.create_all()
    agg_units = [SpatialAggregationUnit(name=f"admin{x}") for x in range(4)]
    agg_units += [
        SpatialAggregationUnit(name="cell"),
        SpatialAggregationUnit(name="site"),
    ]
    db.session.add_all(agg_units)
    users = [User(username="TEST_USER"), User(username="TEST_ADMIN", is_admin=True)]
    for user in users:
        user.password = "DUMMY_PASSWORD"

    # Each user is also a group
    groups = [
        Group(name="TEST_USER", user_group=True),
        Group(name="TEST_ADMIN", user_group=True),
        Group(name="Test_Group"),
    ]
    groups[0].members.append(users[0])
    groups[1].members.append(users[1])
    for user in users:
        groups[2].members.append(user)
    for x in users + groups:
        db.session.add(x)
    # Add some things that you can do
    caps = []
    for c in (
        "available_dates",
        "daily_location",
        "flows",
        "modal_location",
        "location_event_counts",
        "meaningful_locations_aggregate",
        "meaningful_locations_between_label_od_matrix",
        "meaningful_locations_between_dates_od_matrix",
        "geography",
        "unique_subscriber_counts",
        "location_introversion",
        "total_network_objects",
        "aggregate_network_objects",
        "radius_of_gyration",
        "unique_location_counts",
        "subscriber_degree",
    ):
        c = Capability(name=c)
        db.session.add(c)
        caps.append(c)
    # Add some servers
    test_server = Server(
        name="TEST_SERVER",
        longest_token_life=2880,
        latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=365),
    )

    db.session.add(test_server)

    # Add some things that you can do on the servers
    scs = []
    for cap in caps:
        scs.append(
            ServerCapability(
                capability=cap, server=test_server, get_result=True, run=True, poll=True
            )
        )

    for sc in scs:
        sc.spatial_aggregation = agg_units
        db.session.add(sc)
    # Give bob group permissions on Haiti
    for sc in test_server.capabilities:
        gsp = GroupServerPermission(
            group=groups[0], server_capability=sc, get_result=True, run=True, poll=True
        )
        for agg_unit in agg_units[:4]:  # Give Bob access to adminX agg units
            gsp.spatial_aggregation.append(agg_unit)

        db.session.add(gsp)
    db.session.add(
        GroupServerTokenLimits(
            group=groups[0],
            longest_life=1440,
            latest_end=datetime.datetime.now() + datetime.timedelta(days=28),
            server=test_server,
        )
    )
    db.session.commit()
    current_app.config["DB_IS_SET_UP"].set()
    current_app.logger.debug("Made demo data.")


@click.command("demodata")
@with_appcontext
def demodata():
    make_demodata()
    click.echo("Made demo data.")
