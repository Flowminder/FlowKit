# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from sqlalchemy import (
    Column,
    Numeric,
    Text,
    Integer,
    Date,
    Boolean,
    VARCHAR,
    TIMESTAMP,
    String,
    INTEGER,
    LargeBinary,
    DATE,
    JSON,
    ForeignKey,
    Float,
)
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

admin_geo_tables = {}


def admin_geography(*, admin_level: int):
    class_name = f"Admin{admin_level}"
    if class_name in admin_geo_tables:
        return admin_geo_tables[class_name]
    else:
        cls = type(
            class_name,
            (Base,),
            dict(
                __tablename__=f"admin{admin_level}",
                __table_args__=dict(schema="geography"),
                gid=Column("gid", Integer, primary_key=True),
                name=Column(f"admin{admin_level}name", VARCHAR(50)),
                pcod=Column(f"admin{admin_level}pcod", VARCHAR(50)),
                geom=Column("geom", Geometry("MULTIPOLYGON", srid=4326)),
            ),
        )
        admin_geo_tables[class_name] = cls
        return cls


class GeoKinds(Base):
    __tablename__ = "geo_kinds"
    __table_args__ = dict(schema="geography")
    geo_kind_id = Column("geo_kind_id", Integer, primary_key=True)
    name = Column("name", Text)
    geoms = relationship("Geoms", backref="kind")


class GeoLinkageMethods(Base):
    __tablename__ = "linkage_methods"
    __table_args__ = dict(schema="geography")
    linkage_method_id = Column("linkage_method_id", Integer, primary_key=True)
    name = Column("name", Text)
    meta = Column("meta", JSON)
    bridge_entries = relationship("GeoBridge", backref="linkage_method")


class GeoBridge(Base):
    __tablename__ = "geo_bridge"
    __table_args__ = dict(schema="geography")
    location_id = Column(
        "location_id",
        Integer,
        ForeignKey("interactions.locations.location_id"),
        primary_key=True,
    )

    gid = Column("gid", Integer, ForeignKey("geography.geoms.gid"), primary_key=True)
    geom = relationship("Geoms", uselist=False)
    valid_from = Column("valid_from", DATE)
    valid_to = Column("valid_to", DATE)
    weight = Column("weight", Float)
    linkage_method_id = Column(
        "linkage_method_id",
        Integer,
        ForeignKey("geography.linkage_methods.linkage_method_id"),
        primary_key=True,
    )


class Geoms(Base):
    __tablename__ = "geoms"
    __table_args__ = dict(schema="geography")
    gid = Column("gid", Integer, primary_key=True)
    added_date = Column("added_date", TIMESTAMP(timezone=True))
    short_name = Column("short_name", VARCHAR())
    long_name = Column("long_name", Text())
    geo_kind_id = Column(
        "geo_kind_id", Integer, ForeignKey("geography.geo_kinds.geo_kind_id")
    )
    spatial_resolution = Column("spatial_resolution", Integer)
    additional_metadata = Column("additional_metadata", JSON())
    geom = Column("geom", Geometry("MULTIPOLYGON", srid=4326))


class Topups(Base):
    __tablename__ = "topups"
    __table_args__ = dict(schema="events")
    dummy_pk = Column(Integer, primary_key=True)
    id = Column("id", Text())

    datetime = Column("datetime", TIMESTAMP(timezone=True))

    type = Column("type", Text())
    recharge_amount = Column("recharge_amount", Numeric())
    airtime_fee = Column("airtime_fee", Numeric())
    tax_and_fee = Column("tax_and_fee", Numeric())
    pre_event_balance = Column("pre_event_balance", Numeric())
    post_event_balance = Column("post_event_balance", Numeric())

    msisdn = Column("msisdn", Text())

    location_id = Column("location_id", Text())

    imsi = Column("imsi", Text())
    imei = Column("imei", Text())
    tac = Column("tac", Numeric(precision=8, scale=0))

    operator_code = Column("operator_code", Numeric())
    country_code = Column("country_code", Numeric())


class Mds(Base):
    __tablename__ = "mds"
    __table_args__ = dict(schema="events")
    dummy_pk = Column(Integer, primary_key=True)
    id = Column("id", Text())

    datetime = Column("datetime", TIMESTAMP(timezone=True))
    duration = Column("duration", Numeric())

    volume_total = Column("volume_total", Numeric())
    volume_upload = Column("volume_upload", Numeric())
    volume_download = Column("volume_download", Numeric())

    msisdn = Column("msisdn", Text())

    location_id = Column("location_id", Text())

    imsi = Column("imsi", Text())
    imei = Column("imei", Text())
    tac = Column("tac", Numeric(8))

    operator_code = Column("operator_code", Numeric())
    country_code = Column("country_code", Numeric())


class Sms(Base):
    __tablename__ = "sms"
    __table_args__ = dict(schema="events")
    dummy_pk = Column(Integer, primary_key=True)
    id = Column("id", Text())

    outgoing = Column("outgoing", Boolean())

    datetime = Column("datetime", TIMESTAMP(timezone=True))

    network = Column("network", Text())

    msisdn = Column("msisdn", Text())
    msisdn_counterpart = Column("msisdn_counterpart", Text())

    location_id = Column("location_id", Text())

    imsi = Column("imsi", Text())
    imei = Column("imei", Text())
    tac = Column("tac", Numeric(8))

    operator_code = Column("operator_code", Numeric())
    country_code = Column("country_code", Numeric())


class Calls(Base):
    __tablename__ = "calls"
    __table_args__ = dict(schema="events")
    dummy_pk = Column(Integer, primary_key=True)
    id = Column("id", Text())

    outgoing = Column("outgoing", Boolean())

    datetime = Column("datetime", TIMESTAMP(timezone=True))
    duration = Column("duration", Numeric())

    network = Column("network", Text())

    msisdn = Column("msisdn", Text())
    msisdn_counterpart = Column("msisdn_counterpart", Text())

    location_id = Column("location_id", Text())

    imsi = Column("imsi", Text())
    imei = Column("imei", Text())
    tac = Column("tac", Numeric(precision=8, scale=0))

    operator_code = Column("operator_code", Numeric())
    country_code = Column("country_code", Numeric())


class PostETLQueries(Base):
    __tablename__ = "post_etl_queries"
    __table_args_ = dict(schema="etl")
    id = Column("id", Integer, primary_key=True)
    cdr_date = Column("cdr_date", DATE())
    cdr_type = Column("cdr_type", Text())
    type_of_query_or_check = Column("type_of_query_or_check", Text())
    outcome = Column("outcome", Text())
    optional_comment_or_description = Column("optional_comment_or_description", Text())
    timestamp = Column("timestamp", TIMESTAMP(timezone=True))


class ETLRecords(Base):
    __tablename__ = "etl_record"
    __table_args__ = dict(schema="etl")
    id = Column("id", Integer, primary_key=True)
    cdr_type = Column("cdr_type", VARCHAR())
    cdr_date = Column("cdr_date", DATE())
    state = Column("state", VARCHAR())
    timestamp = Column("timestamp", TIMESTAMP(timezone=True))


class CacheConfig(Base):
    __tablename__ = "cache_config"
    __table_args__ = dict(schema="cache")
    key = Column("key", Text, primary_key=True)
    value = Column("value", Text())


class CacheDependencies(Base):
    __tablename__ = "dependencies"
    __table_args__ = dict(schema="cache")
    query_id = Column("query_id", String(32), primary_key=True)
    depends_on = Column("depends_on", String(32), primary_key=True)


class Cached(Base):
    __tablename__ = "cached"
    __table_args__ = dict(schema="cache")
    query_id = Column("query_id", String(32), primary_key=True)
    version = Column("version", VARCHAR())
    query = Column("query", Text())
    created = Column("created", TIMESTAMP(timezone=True))
    access_count = Column("access_count", INTEGER())
    last_accessed = Column("last_accessed", TIMESTAMP(timezone=True))
    compute_time = Column("compute_time", Numeric())
    cache_score_multiplier = Column("cache_score_multiplier", Numeric())
    query_class = Column("class", VARCHAR())
    schema = Column("schema", VARCHAR())
    tablename = Column("tablename", VARCHAR())
    obj = Column("obj", LargeBinary())


class Tacs(Base):
    __tablename__ = "tacs"
    __table_args__ = {"schema": "infrastructure"}
    cell_id = Column("cell_id", Numeric(), primary_key=True)
    id = Column("id", Text())
    version = Column("version", Integer())
    site_id = Column("site_id", Text())
    name = Column("name", Text())
    type = Column("type", Text())
    msc = Column("msc", Text())
    bsc_rnc = Column("bsc_rnc", Text())
    antenna_type = Column("antenna_type", Text())
    status = Column("status", Text())
    lac = Column("lac", Text())
    height = Column("height", Numeric())
    azimuth = Column("azimuth", Numeric())
    transmitter = Column("transmitter", Text())
    max_range = Column("max_range", Numeric())
    min_range = Column("min_range", Numeric())
    electrical_tilt = Column("electrical_tilt", Numeric())
    mechanical_downtilt = Column("mechanical_downtilt", Numeric())
    date_of_first_service = Column("date_of_first_service", Date())
    date_of_last_service = Column("date_of_last_service", Date())


class Sites(Base):
    __tablename__ = "sites"
    __table_args__ = {"schema": "infrastructure"}
    site_id = Column("site_id", Numeric(), primary_key=True)
    id = Column("id", Text())
    version = Column("version", Integer())
    name = Column("name", Text())
    type = Column("type", Text())
    status = Column("status", Text())
    structure_type = Column("structure_type", Text())
    is_cow = Column("is_cow", Boolean())
    date_of_first_service = Column("date_of_first_service", Date())
    date_of_last_service = Column("date_of_last_service", Date())
    geom_polygon = Column("geom_polygon", Geometry("MULTIPOLYGON", srid=4326))
    geom_point = Column("geom_point", Geometry("POINT", srid=4326))


class Cells(Base):
    __tablename__ = "cells"
    __table_args__ = {"schema": "infrastructure"}
    cell_id = Column("cell_id", Numeric(), primary_key=True)
    id = Column("id", Text())
    version = Column("version", Integer())
    site_id = Column("site_id", Text())
    name = Column("name", Text())
    type = Column("type", Text())
    msc = Column("msc", Text())
    bsc_rnc = Column("bsc_rnc", Text())
    antenna_type = Column("antenna_type", Text())
    status = Column("status", Text())
    lac = Column("lac", Text())
    height = Column("height", Numeric())
    azimuth = Column("azimuth", Numeric())
    transmitter = Column("transmitter", Text())
    max_range = Column("max_range", Numeric())
    min_range = Column("min_range", Numeric())
    electrical_tilt = Column("electrical_tilt", Numeric())
    mechanical_downtilt = Column("mechanical_downtilt", Numeric())
    date_of_first_service = Column("date_of_first_service", Date())
    date_of_last_service = Column("date_of_last_service", Date())
    geom_polygon = Column("geom_polygon", Geometry("MULTIPOLYGON", srid=4326))
    geom_point = Column("geom_point", Geometry("POINT", srid=4326))
