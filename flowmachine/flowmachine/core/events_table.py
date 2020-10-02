from flowmachine.core.flowdb_table import FlowDBTable


class EventsTable(FlowDBTable):
    def __init__(self, *, name, columns):
        super().__init__(schema="events", name=name, columns=columns)


class CallsTable(EventsTable):
    all_columns = [
        "id",
        "outgoing",
        "datetime",
        "duration",
        "network",
        "msisdn",
        "msisdn_counterpart",
        "location_id",
        "imsi",
        "imei",
        "tac",
        "operator_code",
        "country_code",
    ]

    def __init__(self, *, columns=None):
        super().__init__(name="calls", columns=columns)


class ForwardsTable(EventsTable):
    all_columns = [
        "id",
        "outgoing",
        "datetime",
        "network",
        "msisdn",
        "msisdn_counterpart",
        "location_id",
        "imsi",
        "imei",
        "tac",
        "operator_code",
        "country_code",
    ]

    def __init__(self, *, columns=None):
        super().__init__(name="forwards", columns=columns)


class SmsTable(EventsTable):
    all_columns = [
        "id",
        "outgoing",
        "datetime",
        "network",
        "msisdn",
        "msisdn_counterpart",
        "location_id",
        "imsi",
        "imei",
        "tac",
        "operator_code",
        "country_code",
    ]

    def __init__(self, *, columns):
        super().__init__(name="sms", columns=columns)


class MdsTable(EventsTable):
    all_columns = [
        "id",
        "datetime",
        "duration",
        "volume_total",
        "volume_upload",
        "volume_download",
        "msisdn",
        "location_id",
        "imsi",
        "imei",
        "tac",
        "operator_code",
        "country_code",
    ]

    def __init__(self, *, columns):
        super().__init__(name="mds", columns=columns)


class TopupsTable(EventsTable):
    all_columns = [
        "id",
        "datetime",
        "type",
        "recharge_amount",
        "airtime_fee",
        "tax_and_fee",
        "pre_event_balance",
        "post_event_balance",
        "msisdn",
        "location_id",
        "imsi",
        "imei",
        "tac",
        "opera" "tor_code",
        "country_code",
    ]

    def __init__(self, *, columns):
        super().__init__(name="topups", columns=columns)


events_table_map = dict(
    calls=CallsTable,
    sms=SmsTable,
    mds=MdsTable,
    topups=TopupsTable,
    forwards=ForwardsTable,
)
