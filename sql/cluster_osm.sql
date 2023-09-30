drop table if exists osm_cluster_temp_result;
drop table if exists osm_clusters;

create temporary table osm_cluster_temp_result as (

     with clusters as (
         select osm.osm_id as osm_id,
                ST_ClusterDBScan(ST_Transform(ST_Centroid(osm.geom), 102023), 250, 5) over () as id,
                --ST_ClusterWithin(ST_Transform(osm.geometry, 102023), 250) over () as id,
                ST_Centroid(osm.geom) as geometry
         from osm
     )

    select
        osm.*,
        c.id as cluster_id
    from osm,
         clusters c
    where osm.osm_id = c.osm_id
);

create table osm_clusters as (

    select
        cluster_id as id,
        -- ST_ConcaveHull(geometry, 0.99) as geometry
        ST_ConvexHull(geometry) as geometry
        -- ST_Centroid(geometry)
    from
        (select cluster_id, ST_Collect(geom) as geometry from osm_cluster_temp_result where cluster_id is not null group by cluster_id) cluster_temp_result

);

