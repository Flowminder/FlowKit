# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path

import datetime
from itertools import chain
from typing import Dict, List, Union

from flask import current_app

import pyotp
from flask_sqlalchemy import SQLAlchemy
from flowauth.invalid_usage import Unauthorized
from flowauth.util import get_fernet
from passlib.hash import argon2
from sqlalchemy.ext.hybrid import hybrid_property

db = SQLAlchemy()


# Link table for mapping users to groups
group_memberships = db.Table(
    "group_memberships",
    db.Column("user_id", db.Integer, db.ForeignKey("user.id"), primary_key=True),
    db.Column("group_id", db.Integer, db.ForeignKey("group.id"), primary_key=True),
)


class User(db.Model):
    """
    A user. Has at least one group.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(75), unique=True, nullable=False)
    _password = db.Column(db.Text, nullable=False)
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

    def allowed_claims(self, server) -> List[str]:
        """
        Get the claims the user is allowed to generate tokens for on a server.

        Parameters
        ----------
        server: Server
            Server to check against

        Returns
        -------
        list of str

        """

        return sorted(
            set(chain.from_iterable(group.rights(server) for group in self.groups))
        )

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
    _secret_key = db.Column(db.Text, nullable=False)  # Encrypted in db
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
        current_app.logger.debug(
            "Verifying 2factor code", code=code, secret_key=self.decrypted_secret_key
        )
        is_valid = pyotp.totp.TOTP(self.decrypted_secret_key).verify(code)
        if is_valid:
            if (
                current_app.config["CACHE_BACKEND"].get(f"{self.user_id}".encode())
                == code
            ):  # Reject if the code is being reused
                raise Unauthorized("Code not valid.")
            else:
                current_app.config["CACHE_BACKEND"].set(
                    f"{self.user_id}".encode(), code
                )
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
                "Failed to decrypt key.", key=key, orig=self._secret_key, exception=exc
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
    _backup_code = db.Column(db.String(75), nullable=False)

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
    name = db.Column(db.String(75), nullable=False)
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
    name = db.Column(db.String(75), unique=True, nullable=False)
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
    The set of API capabilities which are available on a server.
    """

    id = db.Column(db.Integer, primary_key=True)
    server_id = db.Column(db.Integer, db.ForeignKey("server.id"), nullable=False)
    server = db.relationship("Server", back_populates="capabilities", lazy=True)
    capability = db.Column(db.String(21845), nullable=False)
    enabled = db.Column(db.Boolean, default=False)
    group_uses = db.relationship(
        "GroupServerPermission",
        back_populates="server_capability",
        lazy=True,
        cascade="all, delete, delete-orphan",
    )
    __table_args__ = (
        db.UniqueConstraint("server_id", "capability", name="_server_cap_uc"),
    )  # Enforce only one of each capability per server

    def __repr__(self) -> str:
        return f"<ServerCapability {self.capability}@{self.server}>"


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
        return f"<GroupServerPermission {self.server_capability}-{self.group}>"


class Group(db.Model):
    """
    A group of users. Has some set of permissions on some set of servers.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(75), unique=True, nullable=False)
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

    def rights(self, server: Server) -> List[str]:
        return [
            perm.server_capability.capability
            for perm in self.server_permissions
            if perm.server_capability.server.id == server.id
            and perm.server_capability.enabled
        ]

    def __repr__(self) -> str:
        return f"<Group {self.name}>"


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
    with open(Path(__file__).parent / "demo_data" / "api_scopes.txt") as fin:
        caps = [x.strip() for x in fin.readlines()]

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
        sc = ServerCapability(capability=cap, server=test_server, enabled=True)
        scs.append(sc)
        db.session.add(sc)

    # Give bob group permissions on test server
    for sc in test_server.capabilities:
        gsp = GroupServerPermission(group=groups[0], server_capability=sc)
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
