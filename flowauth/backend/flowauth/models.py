# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json
from pathlib import Path
from typing import Dict, List, Union

import flask_migrate
import pyotp
from flask import current_app
from flask_sqlalchemy import SQLAlchemy
from flowauth.invalid_usage import Unauthorized
from flowauth.util import get_fernet
from passlib.hash import argon2
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property

db = SQLAlchemy()


scopes_in_role = db.Table(
    "scopes_in_role",
    db.Column("scope_id", db.Integer, db.ForeignKey("scope.id"), primary_key=True),
    db.Column("role_id", db.Integer, db.ForeignKey("role.id"), primary_key=True),
)

users_with_roles = db.Table(
    "users_with_roles",
    db.Column("user_id", db.Integer, db.ForeignKey("user.id"), primary_key=True),
    db.Column("role_id", db.Integer, db.ForeignKey("role.id"), primary_key=True),
)


class User(db.Model):
    """
    A user.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(75), unique=True, nullable=False)
    _password = db.Column(db.Text, nullable=False)
    is_admin = db.Column(db.Boolean, default=False)

    # Should this table be called 'Roles'?
    roles = db.relationship(
        "Role",
        secondary=users_with_roles,
        lazy="subquery",
        backref=db.backref("users", lazy=True),
    )

    tokens = db.relationship(
        "TokenHistory", back_populates="user", cascade="all, delete, delete-orphan"
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

    def latest_token_expiry(self, server: "Server") -> datetime.datetime:
        """
        Get the latest datetime a token can be valid until on a server.
        Returns the soonest of either the server's expiry date or the
        current time + the servers maximum lifetime.

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

        latest = db.session.execute(
            db.select(Role.latest_token_expiry)
            .where(Role.server_id == server.id)
            .join(User.roles)
            .order_by(Role.latest_token_expiry.desc())
        ).scalar()

        longest = db.session.execute(
            db.select(Role.longest_token_life_minutes)
            .where(Role.server_id == server.id)
            .join(User.roles)
            .order_by(Role.longest_token_life_minutes.desc())
        ).scalar()

        if not latest or not longest:
            raise Unauthorized(f"No roles for {self.username} on {Server.name}")

        return {
            "latest_end": min(server.latest_token_expiry, latest),
            "longest_life": min(server.longest_token_life_minutes, longest),
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
        is_valid = pyotp.totp.TOTP(self.decrypted_secret_key).verify(
            code, valid_window=current_app.config["TWO_FACTOR_VALID_WINDOW"]
        )
        if is_valid:
            if (
                current_app.config["CACHE_BACKEND"].get(
                    f"{self.user_id}-{db.engine.url.database}".encode()
                )
                == code
            ):  # Reject if the code is being reused
                raise Unauthorized("Code not valid.")
            else:
                current_app.config["CACHE_BACKEND"].set(
                    f"{self.user_id}-{db.engine.url.database}".encode(), code
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
    _backup_code = db.Column(db.Text, nullable=False)

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


class Server(db.Model):
    """
    A server. Has a name, and a secret key, and upper bounds on token expiry and lifetime.
    A server has some set of available capabilities.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(75), unique=True, nullable=False)
    latest_token_expiry = db.Column(db.DateTime, nullable=False)
    longest_token_life_minutes = db.Column(db.Integer, nullable=False)

    roles = db.relationship(
        "Role", backref="server", cascade="all, delete, delete-orphan"
    )

    scopes = db.relationship(
        "Scope", backref="server", cascade="all, delete, delete-orphan"
    )

    tokens = db.relationship(
        "TokenHistory", back_populates="server", cascade="all, delete, delete-orphan"
    )

    def next_expiry(self) -> datetime.datetime:
        return min(
            self.latest_token_expiry,
            datetime.datetime.now()
            + datetime.timedelta(minutes=self.longest_token_life_minutes),
        )

    def __repr__(self) -> str:
        return f"<Server {self.name}>"


class Role(db.Model):
    """
    A role assigned to one or more users, providing them with one or more scopes.
    """

    __table_args__ = (UniqueConstraint("name", "server_id"),)

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(75), nullable=False)
    server_id = db.Column(db.Integer, db.ForeignKey("server.id"))
    latest_token_expiry = db.Column(db.DateTime, nullable=False)
    longest_token_life_minutes = db.Column(db.Integer, nullable=False)

    scopes = db.relationship(
        "Scope",
        secondary=scopes_in_role,
        lazy="subquery",
        backref=db.backref("roles", lazy=True),
    )

    def next_expiry(self) -> datetime.datetime:
        return min(
            self.latest_token_expiry,
            datetime.datetime.now()
            + datetime.timedelta(minutes=self.longest_token_life_minutes),
        )

    def allowed_claims(self) -> List[str]:
        """
        Get the claims the role permits access to.

        Returns
        -------
        list of str

        """
        return sorted(self.scopes)

    def is_allowed(self, claims: List[str]) -> bool:
        """
        Returns true if this role permits this combination of claims, else return false.
        """
        scope_strings = [scope.name for scope in self.scopes]

        for claim in claims:
            if claim not in scope_strings:
                return False
        return True

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "scopes": sorted([scope.id for scope in self.scopes]),
            "latest_token_expiry": self.latest_token_expiry.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "longest_token_life_minutes": self.longest_token_life_minutes,
            "server": self.server_id,
            "users": sorted([user.id for user in self.users]),
        }


class Scope(db.Model):
    """
    A scope of actions permitted, represented by a colon-delineated string (fields depend on scope)
    For example, the scope permitting daily locations at admin 3 would be daily_location:admin3
    """

    __table_args__ = (UniqueConstraint("name", "server_id"),)
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(512))
    enabled = db.Column(db.Boolean, default=True)
    server_id = db.Column(db.Integer, db.ForeignKey("server.id"))


class TokenHistory(db.Model):
    """
    All tokens that have been issued by this flowauth instance.
    Each token is owned by one user, applies to one server, has an expiry time, encodes
    several capabilties for a server.
    """

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(75), nullable=False)
    _token = db.Column(db.Text, nullable=False)
    expiry = db.Column(db.DateTime, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    user = db.relationship("User", back_populates="tokens", lazy=True)
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
        # Because we're starting fresh, we don't need to migrate and can mark the
        # created schema as current
        current_app.logger.debug("Dropping existing db.")
        db.drop_all()
        db.create_all()
        flask_migrate.stamp()

    flask_migrate.upgrade()
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
    init_db(force=True)
    current_app.logger.debug("Creating demo data.")

    # Add some servers
    test_server = Server(
        name="TEST_SERVER",
        longest_token_life_minutes=31 * 24 * 60,
        latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=365),
    )
    db.session.add(test_server)

    with (Path(__file__).parent / Path("demo_data/demo_scopes.json")).open() as spec:
        scopes_doc = json.load(spec)
    scopes = {
        scope_string: Scope(name=scope_string, server=test_server)
        for scope_string in scopes_doc["components"]["securitySchemes"]["token"][
            "x-security-scopes"
        ]
    }

    reader_scope = scopes["get_result"]
    runner_scope = scopes["run"]
    dates_scope = scopes["get_available_dates"]
    complex_scopes = [scope for scope in scopes.values() if ":" in scope.name]
    db.session.add_all(scopes.values())

    # Add roles to test server
    roles = [
        Role(
            name="viewer",
            server=test_server,
            scopes=[reader_scope, dates_scope, *complex_scopes],
            latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=30),
            longest_token_life_minutes=30 * 24 * 60,
        ),
        Role(
            name="runner",
            server=test_server,
            scopes=[runner_scope, dates_scope, *complex_scopes],
            latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=30),
            longest_token_life_minutes=30 * 24 * 60,
        ),
    ]
    db.session.add_all(roles)

    users = [User(username="TEST_USER"), User(username="TEST_ADMIN", is_admin=True)]

    for user in users:
        user.password = "DUMMY_PASSWORD"
        user.roles += roles

    db.session.add_all(users)

    test_token_history = TokenHistory(
        name="Example token",
        user_id=2,
        expiry=datetime.datetime.now() + datetime.timedelta(days=365),
        token="If you're reading this, the token history might be working",
        server_id=1,
    )
    db.session.add(test_token_history)

    db.session.commit()
    current_app.config["DB_IS_SET_UP"].set()
    current_app.logger.debug("Made demo data.")
