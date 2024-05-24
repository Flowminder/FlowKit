from flowmachine.core.flowdb_table import FlowDBTable


class InfrastructureTable(FlowDBTable):
    def __init__(self, *, name, columns):
        super().__init__(schema="infrastructure", name=name, columns=columns)


class SitesTable(InfrastructureTable):
    all_columns = [
        "site_id",
        "id",
        "version",
        "name",
        "type",
        "status",
        "structure_type",
        "is_cow",
        "date_of_first_service",
        "date_of_last_service",
        "geom_point",
        "geom_polygon",
    ]

    def __init__(self, *, columns=None):
        super().__init__(name="sites", columns=columns)


class CellsTable(InfrastructureTable):
    all_columns = [
        "cell_id",
        "id",
        "version",
        "site_id",
        "name",
        "type",
        "msc",
        "bsc_rnc",
        "antenna_type",
        "status",
        "lac",
        "height",
        "azimuth",
        "transmitter",
        "max_range",
        "min_range",
        "electrical_tilt",
        "mechanical_downtilt",
        "date_of_f" "irst_service",
        "date_of_last_service",
        "geom_point",
        "geom_polygon",
    ]

    def __init__(self, *, columns=None):
        super().__init__(name="cells", columns=columns)


class TacsTable(InfrastructureTable):
    all_columns = [
        "id",
        "brand",
        "model",
        "width",
        "height",
        "depth",
        "weight",
        "display_type",
        "display_colors",
        "display_width",
        "display_height",
        "mms_receiver",
        "mms_built_in_camera",
        "wap_push_ota_support",
        "hardware_gprs",
        "hardw" "are_edge",
        "hardware_umts",
        "hardware_wifi",
        "hardware_bluetooth",
        "hardware_gps",
        "software_os_vendor",
        "software_os_name",
        "software_os_version",
        "wap_push_ota_settings",
        "wap_push_ota_bookmarks",
        "wap_push_ota" "_app_internet",
        "wap_push_ota_app_browser",
        "wap_push_ota_app_mms",
        "wap_push_ota_single_shot",
        "wap_push_ota_multi_shot",
        "wap_push_oma_settings",
        "wap_push_oma_app_internet",
        "wap_push_oma_app_browser",
        "wap_" "push_oma_app_mms",
        "wap_push_oma_cp_bookmarks",
        "wap_1_2_1",
        "wap_2_0",
        "syncml_dm_settings",
        "syncml_dm_acc_gprs",
        "syncml_dm_app_internet",
        "syncml_dm_app_browser",
        "syncml_dm_app_mms",
        "syncml_dm_app_bookmark",
        "syncml_dm_app_java",
        "wap_push_oma_app_ims",
        "wap_push_oma_app_poc",
        "j2me_midp_10",
        "j2me_midp_20",
        "j2me_midp_21",
        "j2me_cldc_10",
        "j2me_cldc_11",
        "j2me_cldc_20",
        "hnd_type",
    ]

    def __init__(self, *, columns=None):
        super().__init__(name="tacs", columns=columns)


infrastructure_table_map = dict(tacs=TacsTable, cells=CellsTable, sites=SitesTable)
