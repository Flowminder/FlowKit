from typing import Dict, List

from flowmachine.features import LabelEventScore
from . import HartiganCluster, EventScore
from ...core import Query


class DayNightFlow(Query):
    def __init__(
        self,
        *,
        clusters: HartiganCluster,
        scores: EventScore,
        labels: Dict[str, List[Dict[str, float]]],
    ):
        labelled_hartigan = LabelEventScore(
            clusters.join_to_cluster_components(scores), labels=labels
        )
        work = labelled_hartigan.subset("label", "work")
        home = labelled_hartigan.subset("label", "home")
        self.joined = home.join(
            work, on_left="msisdn", on_right="msisdn", right_append="_work", how="inner"
        )
        self.level = level

        super().__init__()

    def _geo_augmented_query(self):
        units = "select geom_square as geom, grid_id as gid from ({}) as clusts, geography.admin0 WHERE ST_Intersects(geom_square, geography.admin0.geom)".format(
            self.level.get_query()
        )

        jsoned = """
        WITH joined as (select * from ({gidded}) g where g.count > 15)
        SELECT geom, gid, gid as name, total_outflows, total_inflows, inflows, outflows FROM ({units}) AS cs 
        LEFT JOIN LATERAL (
            select json_object_agg(t2.gid_to, t2.count) as outflows,
                   json_object_agg(t.gid_from, t.count) as inflows,
                   sum(t2.count) as total_outflows, sum(t.count) as total_inflows
            FROM
                (
                  select gid_from, count from joined WHERE gid_to=cs.gid
                ) t,
                (
                  select gid_to, count from joined WHERE gid_from=cs.gid
                ) t2
            ) z ON TRUE
            where  (inflows notnull or outflows notnull)
        """.format(
            gidded=self.get_query(), units=units
        )
        return jsoned

    def _make_query(self):
        return """
        SELECT COUNT(*) as count, h.grid_id as gid_from, w.grid_id as gid_to FROM ({}) as j
        LEFT JOIN ({level}) h
            ON ST_Intersects(cluster, h.geom_square)
        LEFT JOIN ({level}) w
            ON ST_Intersects(cluster_work, w.geom_square)
        GROUP BY gid_from, gid_to ORDER BY count DESC
        """.format(
            self.joined.get_query(), level=self.level.get_query()
        )
