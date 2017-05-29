'use strict';
const logger = require('logger');
const path = require('path');
const config = require('config');
const CartoDB = require('cartodb');
const Mustache = require('mustache');
const NotFound = require('errors/notFound');
const GeostoreService = require('services/geostoreService');
const JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

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

const ISO = `with r as (SELECT date,pais,sup, prov_dep FROM gran_chaco_deforestation),
             d as (SELECT iso, name_0 FROM gadm2_countries_simple WHERE iso = UPPER('{{iso}}')),
             f as (select * from r right join d on pais=name_0 AND date >= '{{begin}}'::date
             AND date <= '{{end}}'::date)
        SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
        FROM f`;

const ID1 = ` with r as (SELECT date,pais,sup, prov_dep FROM gran_chaco_deforestation),
              d as (SELECT name_1, iso, id_1, name_0 FROM gadm2_provinces_simple WHERE iso = UPPER('{{iso}}') AND id_1 = {{id1}}),
              f as (select * from r right join d on prov_dep=name_1 AND date >= '{{begin}}'::date
              AND date <= '{{end}}'::date)
        SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
        FROM f`;

const USE = `SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
        FROM {{useTable}} u inner join gran_chaco_deforestation f
        on ST_Intersects(f.the_geom, u.the_geom) AND date >= '{{begin}}'::date
        AND date <= '{{end}}'::date
        WHERE u.cartodb_id = {{pid}}`;

const WDPA = `WITH p as (SELECT CASE
              WHEN marine::numeric = 2 then null
              WHEN ST_NPoints(the_geom)<=18000 THEN the_geom
              WHEN ST_NPoints(the_geom) BETWEEN 18000 AND 50000 THEN ST_RemoveRepeatedPoints(the_geom, 0.001)
              ELSE ST_RemoveRepeatedPoints(the_geom, 0.005)
             END as the_geom, gis_area*100 as area_ha FROM wdpa_protected_areas where wdpaid={{wdpaid}})
        SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
        FROM gran_chaco_deforestation f inner join p
        ON ST_Intersects(f.the_geom, p.the_geom)
        AND date >= '{{begin}}'::date
              AND date <= '{{end}}'::date`;

const LATEST = `SELECT DISTINCT date
        FROM gran_chaco_deforestation
        ORDER BY date DESC
        LIMIT {{limit}}`;

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

let getToday = function() {
    let today = new Date();
    return `${today.getFullYear().toString()}-${(today.getMonth()+1).toString()}-${today.getDate().toString()}`;
};

let getYesterday = function() {
    let yesterday = new Date(Date.now() - (24 * 60 * 60 * 1000));
    return `${yesterday.getFullYear().toString()}-${(yesterday.getMonth()+1).toString()}-${yesterday.getDate().toString()}`;
};


let defaultDate = function() {
    let to = getToday();
    let from = getYesterday();
    return from + ',' + to;
};

class CartoDBService {

    constructor() {
        this.client = new CartoDB.SQL({
            user: config.get('cartoDB.user')
        });
        this.apiUrl = config.get('cartoDB.apiUrl');
    }

    getDownloadUrls(query, params) {
        try{
            let formats = ['csv', 'geojson', 'kml', 'shp', 'svg'];
            let download = {};
            let queryFinal = Mustache.render(query, params);
            queryFinal = queryFinal.replace('sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha', 'f.*');
            queryFinal = queryFinal.replace('sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date', 'f.*');
            queryFinal = encodeURIComponent(queryFinal);
            for(let i=0, length = formats.length; i < length; i++){
                download[formats[i]] = this.apiUrl + '?q=' + queryFinal + '&format=' + formats[i];
            }
            return download;
        }catch(err){
            logger.error(err);
        }
    }

    getPeriodText(period) {
        let periods = period.split(',');
        let days = (new Date(periods[1]) - new Date(periods[0])) / (24 * 60 * 60 * 1000);

        switch (days) {
            case 1:
                return 'Past 24 hours';
            case 2:
                return 'Past 48 hours';
            case 3:
                return 'Past 72 hours';
            default:
                return 'Past week';
        }
    }

    * getNational(iso, period = defaultDate()) {
        logger.debug('Obtaining national of iso %s', iso);
        let periods = period.split(',');
        let params = {
            iso: iso,
            begin: periods[0],
            end: periods[1]
        };
        const geostore = yield GeostoreService.getGeostoreByIso(iso);

        let data = yield executeThunk(this.client, ISO, params);
        if (geostore) {
            if (data.rows && data.rows.length > 0) {
                let result = data.rows[0];
                result.area_ha = geostore.areaHa;
                result.period = this.getPeriodText(period);
                result.downloadUrls = this.getDownloadUrls(ISO, params);
                return result;
            } else {
                return {
                    area_ha: geostore.areaHa   
                };
            }
        }
        return null;
    }

    * getSubnational(iso, id1, period = defaultDate()) {
        logger.debug('Obtaining subnational of iso %s and id1', iso, id1);
        let periods = period.split(',');
        let params = {
            iso: iso,
            id1: id1,
            begin: periods[0],
            end: periods[1]
        };
        const geostore = yield GeostoreService.getGeostoreByIsoAndId(iso, id1);
        let data = yield executeThunk(this.client, ID1, params);
        if (geostore) {
            if (data.rows && data.rows.length > 0) {
                let result = data.rows[0];
                result.area_ha = geostore.areaHa;
                result.period = this.getPeriodText(period);
                result.downloadUrls = this.getDownloadUrls(ID1, params);
                return result;
            } else {
                return {
                    area_ha: geostore.areaHa   
                };
            }
        }
        return null;
    }

    * getUse(useName, useTable, id, period = defaultDate()) {
        logger.debug('Obtaining use with id %s', id);
        let periods = period.split(',');
        let params = {
            useTable: useTable,
            pid: id,
            begin: periods[0],
            end: periods[1]
        };

        const geostore = yield GeostoreService.getGeostoreByUse(useName, id);
        let data = yield executeThunk(this.client, USE, params);
        if (geostore) {
            if (data.rows && data.rows.length > 0) {
                let result = data.rows[0];
                result.area_ha = geostore.areaHa;
                result.period = this.getPeriodText(period);
                result.downloadUrls = this.getDownloadUrls(USE, params);
                return result;
            } else {
                return {
                    area_ha: geostore.areaHa   
                };
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

        const geostore = yield GeostoreService.getGeostoreByWdpa(wdpaid);
        let data = yield executeThunk(this.client, WDPA, params);
        if (geostore) {
            if (data.rows && data.rows.length > 0) {
                let result = data.rows[0];
                result.area_ha = geostore.areaHa;
                result.period = this.getPeriodText(period);
                result.downloadUrls = this.getDownloadUrls(WDPA, params);
                return result;
            } else {
                return {
                    area_ha: geostore.areaHa   
                };
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
        if (data.rows) {
            let result = data.rows[0];
            if(data.rows.length > 0){
                result.area_ha = data.rows[0].area_ha;
            }
            if (areaHa) {
                result.area_ha = areaHa;
            }
            result.downloadUrls = this.getDownloadUrls(WORLD, params);
            return result;
        }
        return null;
    }

    * latest(limit=3) {
        logger.debug('Obtaining latest with limit %s', limit);
        let params = {
            limit: limit
        };
        let data = yield executeThunk(this.client, LATEST, params);

        if (data.rows ) {
            let result = data.rows;
            return result;
        }
        return null;
    }

}

module.exports = new CartoDBService();
