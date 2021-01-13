# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_macros():
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    class MixTo:
        def render_template_fields(self, context, *, jinja_env):
            self.context = context

    class MixedIn(TableNameMacrosMixin, MixTo):
        pass

    test_context = dict(params=dict(cdr_type="TEST_TYPE"), ds_nodash="DATE_STAMP")
    mixed = MixedIn()
    mixed.render_template_fields(context=test_context, jinja_env=None)
    assert mixed.context == dict(
        parent_table="events.TEST_TYPE",
        table_name="TEST_TYPE_DATE_STAMP",
        etl_schema="etl",
        final_schema="events",
        extract_table_name="extract_TEST_TYPE_DATE_STAMP",
        staging_table_name="stg_TEST_TYPE_DATE_STAMP",
        final_table="events.TEST_TYPE_DATE_STAMP",
        extract_table="etl.extract_TEST_TYPE_DATE_STAMP",
        staging_table="etl.stg_TEST_TYPE_DATE_STAMP",
        cdr_type="TEST_TYPE",
        **test_context,
    )
