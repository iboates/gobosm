local tables = {}

tables.buildings = osm2pgsql.define_table({
    name = 'osm',
    ids = { type = 'any', id_column = 'osm_id', type_column = 'osm_type' },
    columns = {
        { column = 'geom', type = 'point', projection = 4326, not_null = true }
    }
})

function osm2pgsql.process_way(object)
    if object.is_closed then
        if object.tags.building ~= nil then
            local building = {}
            building.geom = object:as_polygon():centroid()
            buildings:insert(building)
        end
    end
end
