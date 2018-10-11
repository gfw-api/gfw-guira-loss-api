'use strict';
const logger = require('logger');
const path = require('path');
const config = require('config');
const CartoDB = require('cartodb');
const Mustache = require('mustache');
const NotFound = require('errors/notFound');
const GeostoreService = require('services/geostoreService');

const WORLD = `
        with p as (select ST_Area(ST_SetSRID(ST_GeomFromGeoJSON('{{{geojson}}}'), 4326), TRUE)/1000 as area_ha ),
        c  as (SELECT sum(st_area(st_intersection(ST_SetSRID(
                  ST_GeomFromGeoJSON('{{{geojson}}}'), 4326), f.the_geom), true)/10000) as value, MIN(date) as min_date, MAX(date) as max_date
        FROM gran_chaco_deforestation f
        WHERE date >= '{{begin}}'::date
              AND date <= '{{end}}'::date
              AND ST_INTERSECTS(
                ST_SetSRID(ST_GeomFromGeoJSON('{{{geojson}}}'), 4326), f.the_geom)
        )
        SELECT  c.value, p.area_ha
        FROM c, p`;

const AREA = `select ST_Area(ST_SetSRID(ST_GeomFromGeoJSON('{{{geojson}}}'), 4326), TRUE)/10000 as area_ha`;

const ISO = `with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, iso, name_0, area_ha FROM gadm36_countries WHERE iso = UPPER('{{iso}}')),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const ID1 = ` with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, name_1, iso, gid_1, name_0, area_ha FROM gadm36_adm1 WHERE iso = UPPER('{{iso}}') AND gid_1 = '{{id1}}'),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const ID2 = ` with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, name_1, iso, gid_1, name_0, gid_2, name_2, area_ha FROM gadm36_adm2 WHERE iso = UPPER('{{iso}}') AND gid_1 = '{{id1}}' AND gid_2 = '{{id2}}'),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const USEAREA = `select area_ha FROM {{useTable}} WHERE cartodb_id = {{pid}}`;

const USE = `SELECT area_ha, sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
FROM {{useTable}} u inner join gran_chaco_deforestation f
on ST_Intersects(f.the_geom, u.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date
WHERE u.cartodb_id = {{pid}} GROUP BY u.area_ha`;

const WDPAAREA = `select gis_area*100 as area_ha FROM wdpa_protected_areas WHERE wdpaid = {{wdpaid}}`;

const WDPA = `WITH p as (SELECT CASE
              WHEN marine::numeric = 2 then null
              WHEN ST_NPoints(the_geom)<=18000 THEN the_geom
              WHEN ST_NPoints(the_geom) BETWEEN 18000 AND 50000 THEN ST_RemoveRepeatedPoints(the_geom, 0.001)
              ELSE ST_RemoveRepeatedPoints(the_geom, 0.005)
             END as the_geom, gis_area*100 as area_ha FROM wdpa_protected_areas where wdpaid={{wdpaid}})
        SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
        FROM gran_chaco_deforestation f inner join p
        ON ST_Intersects(f.the_geom, p.the_geom)
        AND date >= '{{begin}}'::date
              AND date <= '{{end}}'::date GROUP BY area_ha`;

const LATEST = `with a AS (SELECT DISTINCT date
    FROM gran_chaco_deforestation
    WHERE date IS NOT NULL) SELECT MAX(date) AS latest FROM a`;

var executeThunk = function(client, sql, params) {
    return function(callback) {
        logger.debug(Mustache.render(sql, params));
        client.execute(sql, params).done(function(data) {
            callback(null, data);
        }).error(function(err) {
            callback(err, null);
        });
    };
};

const routeToGid = function (adm0, adm1, adm2) {
    return {
        adm0,
        adm1: adm1 ? `${adm0}.${adm1}_1` : null,
        adm2: adm2 ? `${adm0}.${adm1}.${adm2}_1` : null
    };
};

let getToday = function() {
    let today = new Date();
    return `${today.getFullYear().toString()}-${(today.getMonth()+1).toString()}-${today.getDate().toString()}`;
};

let defaultDate = function() {
    let to = getToday();
    let from = '2011-09-01';
    return from + ',' + to;
};

const getSimplify = (iso) => {
    let thresh = 0.005;
    if (iso) {
      const bigCountries = ['USA', 'RUS', 'CAN', 'CHN', 'BRA', 'IDN'];
      thresh = bigCountries.includes(iso) ? 0.05 : 0.005;
    }
    return thresh;
  };

class CartoDBServiceV2 {

    constructor() {
        this.client = new CartoDB.SQL({
            user: config.get('cartoDB.user')
        });
        this.apiUrl = config.get('cartoDB.apiUrl');
    }

    getDownloadUrls(query, params) {
        try{
            let formats = ['csv', 'json', 'kml', 'shp', 'svg'];
            let download = {};
            let queryFinal = Mustache.render(query, params);
            queryFinal = encodeURIComponent(queryFinal);
            for(let i=0, length = formats.length; i < length; i++){
                download[formats[i]] = this.apiUrl + '?q=' + queryFinal + '&format=' + formats[i];
            }
            return download;
        }catch(err){
            logger.error(err);
        }
    }

    * getAdm0(iso, period = defaultDate()) {
        logger.debug('Obtaining national of iso %s', iso);
        const gid = routeToGid(iso);
        const simplify = getSimplify(iso);
        let periods = period.split(',');
        let params = {
            iso: gid.adm0,
            begin: periods[0],
            end: periods[1],
            simplify
        };
        let data = yield executeThunk(this.client, ISO, params);
        if (data && data.rows && data.rows.length > 0) {
            let result = data.rows[0];
            result.area_ha = result.area_ha;
            result.period = period;
            result.id = params.iso;
            result.downloadUrls = this.getDownloadUrls(ISO, params);
            return result;
        }
        return null;
    }

    * getAdm1(iso, id1, period = defaultDate()) {
    logger.debug('Obtaining subnational of iso %s and id1', iso, id1);
    const gid = routeToGid(iso, id1);
    const simplify = getSimplify(iso) / 10;
    let periods = period.split(',');
    let params = {
        iso: gid.adm0,
        id1: gid.adm1,
        begin: periods[0],
        end: periods[1],
        simplify
    };
    let data = yield executeThunk(this.client, ID1, params);
    if (data && data.rows && data.rows.length > 0) {
        let result = data.rows[0];
        result.area_ha = result.area_ha;
        result.period = period;
        result.id = gid.adm1;
        result.downloadUrls = this.getDownloadUrls(ID1, params);
        return result;
    }
    return null;
}

* getAdm2(iso, id1, id2, period = defaultDate()) {
    logger.debug('Obtaining subnational of iso %s and id1', iso, id1);
    const gid = routeToGid(iso, id1, id2);
    const simplify = getSimplify(iso) / 100;
    let periods = period.split(',');
    let params = {
        iso: gid.adm0,
        id1: gid.adm1,
        id2: gid.adm2,
        begin: periods[0],
        end: periods[1],
        simplify
    };
    let data = yield executeThunk(this.client, ID2, params);
    if (data && data.rows && data.rows.length > 0) {
        let result = data.rows[0];
        result.area_ha = result.area_ha;
        result.period = period;
        result.id = gid.adm2;
        result.downloadUrls = this.getDownloadUrls(ID2, params);
        return result;
    }
    return null;
}


    * getUse(useTable, id, period = defaultDate()) {
        logger.debug('Obtaining use with id %s', id);
        let periods = period.split(',');
        let params = {
            useTable: useTable,
            pid: id,
            begin: periods[0],
            end: periods[1]
        };

        let data = yield executeThunk(this.client, USE, params);
        if (data.rows && data.rows.length > 0) {
            let result = data.rows[0];
            result.id = id;
            result.period = this.getPeriodText(period);
            result.downloadUrls = this.getDownloadUrls(USE, params);
            return result;
        }
        let areas = yield executeThunk(this.client, USEAREA, params);
        if (areas.rows && areas.rows.length > 0) {
            let result = areas.rows[0];
            result.id = id;
            result.value = 0;
            return result;
        }
        const geostore = yield GeostoreService.getGeostoreByUse(useName, id);
        if(geostore){
            return {
                id, id,
                value: 0,
                area_ha: geostore.area_ha
            }
        }
        return null;
    }

    * getWdpa(wdpaid, period = defaultDate()) {
        logger.debug('Obtaining wpda of id %s', wdpaid);
        let periods = period.split(',');
        let params = {
            wdpaid: wdpaid,
            begin: periods[0],
            end: periods[1]
        };

        let data = yield executeThunk(this.client, WDPA, params);
        if (data.rows && data.rows.length > 0) {
            let result = data.rows[0];
            result.id = wdpaid;
            result.period = period;
            result.downloadUrls = this.getDownloadUrls(WDPA, params);
            return result;
        }
        let areas = yield executeThunk(this.client, WDPAAREA, params);
        if (areas.rows && areas.rows.length > 0) {
            let result = areas.rows[0];
            result.id = wdpaid;
            result.value = 0;
            return result;
        }
        const geostore = yield GeostoreService.getGeostoreByUse(useName, id);
        if(geostore){
            return {
                id: wdpaid,
                value: 0,
                area_ha: geostore.area_ha
            }
        }
        return null;
    }

    * getWorld(hashGeoStore, period = defaultDate()) {
        logger.debug('Obtaining world with hashGeoStore %s', hashGeoStore);

        const geostore = yield GeostoreService.getGeostoreByHash(hashGeoStore);
        if (geostore && geostore.geojson) {
            return yield this.getWorldWithGeojson(geostore.geojson, geostore.areaHa, period);
        }
        throw new NotFound('Geostore not found');
    }

    * getWorldWithGeojson(geojson, areaHa, period = defaultDate()) {
        logger.debug('Executing query in cartodb with geojson', geojson);
        let periods = period.split(',');
        let params = {
            geojson: JSON.stringify(geojson.features[0].geometry),
            begin: periods[0],
            end: periods[1]
        };
        let data = yield executeThunk(this.client, WORLD, params);
        let dataArea = yield executeThunk(this.client, AREA, params);
        let result = {
            area_ha: dataArea.rows[0].area_ha
        };
        if (data.rows) {
            result.value = data.rows[0].value || 0;

        }
        result.area_ha = dataArea.rows[0].area_ha;
        result.downloadUrls = this.getDownloadUrls(WORLD, params);
        return result;
        
    }

    * latest() {
    logger.debug('Obtaining latest date');
    let data = yield executeThunk(this.client, LATEST);
    if (data && data.rows && data.rows.length) {
        let result = data.rows;
        return result;
    }
    return null;
}

}

module.exports = new CartoDBServiceV2();
