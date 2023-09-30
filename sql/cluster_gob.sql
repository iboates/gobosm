drop table if exists gob_cluster_temp_result;
drop table if exists gob_clusters;

create temporary table gob_cluster_temp_result as (

     with clusters as (
         select gob.id as gob_id,
                ST_ClusterDBScan(ST_Transform(ST_Centroid(gob.geometry), 102023), 250, 5) over () as id,
                --ST_ClusterWithin(ST_Transform(gob.geometry, 102023), 250) over () as id,
                ST_Centroid(gob.geometry) as geometry
         from gob
         where confidence > 0
     )

    select
        gob.*,
        c.id as cluster_id
    from gob,
         clusters c
    where gob.id = c.gob_id
);

create table gob_clusters as (

    select
        cluster_id as id,
        -- ST_ConcaveHull(geometry, 0.99) as geometry
        ST_ConvexHull(geometry) as geometry
        -- ST_Centroid(geometry)
    from
        (select cluster_id, ST_Collect(geometry) as geometry from gob_cluster_temp_result where cluster_id is not null group by cluster_id) cluster_temp_result

);

