import config from 'config';
import logger from 'logger';
import Mustache from 'mustache';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import CartoDB from 'cartodb';
import GeostoreService from 'services/geostoreService';
import NotFound from 'errors/notFound';

const WORLD: string = `
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

const AREA: string = `select ST_Area(ST_SetSRID(ST_GeomFromGeoJSON('{{{geojson}}}'), 4326), TRUE)/10000 as area_ha`;

const ISO: string = `with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, iso, name_0, area_ha FROM gadm36_countries WHERE iso = UPPER('{{iso}}')),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const ID1: string = ` with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, name_1, iso, gid_1, name_0, area_ha FROM gadm36_adm1 WHERE iso = UPPER('{{iso}}') AND gid_1 = '{{id1}}'),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const ID2: string = ` with r as (SELECT date,pais,sup, the_geom FROM gran_chaco_deforestation),
d as (SELECT ST_makevalid(ST_simplify(the_geom, {{simplify}})) AS the_geom, name_1, iso, gid_1, name_0, gid_2, name_2, area_ha FROM gadm36_adm2 WHERE iso = UPPER('{{iso}}') AND gid_1 = '{{id1}}' AND gid_2 = '{{id2}}'),
f as (select * from r right join d on ST_intersects(r.the_geom, d.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date)
SELECT sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date, area_ha
FROM f GROUP BY area_ha`;

const USEAREA: string = `select area_ha FROM {{useTable}} WHERE cartodb_id = {{pid}}`;

const USE: string = `SELECT area_ha, sum(sup) AS value, MIN(date) as min_date, MAX(date) as max_date
FROM {{useTable}} u inner join gran_chaco_deforestation f
on ST_Intersects(f.the_geom, u.the_geom) AND date >= '{{begin}}'::date
AND date <= '{{end}}'::date
WHERE u.cartodb_id = {{pid}} GROUP BY u.area_ha`;

const WDPAAREA: string = `select gis_area*100 as area_ha FROM wdpa_protected_areas WHERE wdpaid = {{wdpaid}}`;

const WDPA: string = `WITH p as (SELECT CASE
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

const LATEST: string = `with a AS (SELECT DISTINCT date
    FROM gran_chaco_deforestation
    WHERE date IS NOT NULL) SELECT MAX(date) AS latest FROM a`;

const executeThunk = async (client: CartoDB.SQL, sql: string, params: any): Promise<Record<string, any>> => (new Promise((resolve: (value: (PromiseLike<unknown> | unknown)) => void, reject: (reason?: any) => void) => {
    logger.debug(Mustache.render(sql, params));
    client.execute(sql, params).done((data: Record<string, any>) => {
        resolve(data);
    }).error((error: Error) => {
        reject(error);
    });
}));

const routeToGid = (adm0: string, adm1?: string, adm2?: string): Record<string, any> => ({
    adm0,
    adm1: adm1 ? `${adm0}.${adm1}_1` : null,
    adm2: adm2 ? `${adm0}.${adm1}.${adm2}_1` : null
});

const getToday = (): string => {
    const today: Date = new Date();
    return `${today.getFullYear().toString()}-${(today.getMonth() + 1).toString()}-${today.getDate().toString()}`;
};

const defaultDate = (): string => {
    const to: string = getToday();
    const from: string = '2011-09-01';
    return `${from},${to}`;
};

const getSimplify = (iso: string): number => {
    let thresh: number = 0.005;
    if (iso) {
        const bigCountries: string[] = ['USA', 'RUS', 'CAN', 'CHN', 'BRA', 'IDN'];
        thresh = bigCountries.includes(iso) ? 0.05 : 0.005;
    }
    return thresh;
};

const getPeriodText = (period: string): string => {
    const periods: string[] = period.split(',');
    const days: number = (new Date(periods[1]).getTime() - new Date(periods[0]).getTime()) / (24 * 60 * 60 * 1000);

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
};

class CartoDBServiceV2 {

    client: CartoDB.SQL;
    apiUrl: string;

    constructor() {
        this.client = new CartoDB.SQL({
            user: config.get('cartoDB.user')
        });
        this.apiUrl = config.get('cartoDB.apiUrl');
    }

    getDownloadUrls(query: string, params: Record<string, any>): Record<string, any> | void {
        try {
            const formats: string[] = ['csv', 'json', 'kml', 'shp', 'svg'];
            const download: Record<string, any> = {};
            let queryFinal: string = Mustache.render(query, params);
            queryFinal = encodeURIComponent(queryFinal);
            for (let i: number = 0, { length } = formats; i < length; i++) {
                download[formats[i]] = `${this.apiUrl}?q=${queryFinal}&format=${formats[i]}`;
            }
            return download;
        } catch (err) {
            logger.error(err);
        }
    }

    async getAdm0(iso: string, period: string = defaultDate()): Promise<Record<string, any> | void> {
        logger.debug('Obtaining national of iso %s', iso);
        const gid: Record<string, any> = routeToGid(iso);
        const simplify: number = getSimplify(iso);
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            iso: gid.adm0,
            begin: periods[0],
            end: periods[1],
            simplify
        };
        const data: Record<string, any> = await executeThunk(this.client, ISO, params);
        if (data && data.rows && data.rows.length > 0) {
            const result: Record<string, any> = data.rows[0];
            result.period = period;
            result.id = params.iso;
            result.downloadUrls = this.getDownloadUrls(ISO, params);
            return result;
        }
        return null;
    }

    async getAdm1(iso: string, id1: string, period: string = defaultDate()): Promise<Record<string, any> | void> {
        logger.debug('Obtaining subnational of iso %s and id1', iso, id1);
        const gid: Record<string, any> = routeToGid(iso, id1);
        const simplify: number = getSimplify(iso) / 10;
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            iso: gid.adm0,
            id1: gid.adm1,
            begin: periods[0],
            end: periods[1],
            simplify
        };
        const data: Record<string, any> = await executeThunk(this.client, ID1, params);
        if (data && data.rows && data.rows.length > 0) {
            const result: Record<string, any> = data.rows[0];
            result.period = period;
            result.id = gid.adm1;
            result.downloadUrls = this.getDownloadUrls(ID1, params);
            return result;
        }
        return null;
    }

    async getAdm2(iso: string, id1: string, id2: string, period: string = defaultDate()): Promise<Record<string, any> | void> {
        logger.debug('Obtaining subnational of iso %s and id1', iso, id1);
        const gid: Record<string, any> = routeToGid(iso, id1, id2);
        const simplify: number = getSimplify(iso) / 100;
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            iso: gid.adm0,
            id1: gid.adm1,
            id2: gid.adm2,
            begin: periods[0],
            end: periods[1],
            simplify
        };
        const data: Record<string, any> = await executeThunk(this.client, ID2, params);
        if (data && data.rows && data.rows.length > 0) {
            const result: Record<string, any> = data.rows[0];
            result.period = period;
            result.id = gid.adm2;
            result.downloadUrls = this.getDownloadUrls(ID2, params);
            return result;
        }
        return null;
    }


    async getUse(useTable: string, id: string, period: string = defaultDate(), apiKey: string): Promise<Record<string, any> | void> {
        logger.debug('Obtaining use with id %s', id);
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            useTable,
            pid: id,
            begin: periods[0],
            end: periods[1]
        };

        const data: Record<string, any> = await executeThunk(this.client, USE, params);
        if (data.rows && data.rows.length > 0) {
            const result: Record<string, any> = data.rows[0];
            result.id = id;
            result.period = getPeriodText(period);
            result.downloadUrls = this.getDownloadUrls(USE, params);
            return result;
        }
        const areas: Record<string, any> = await executeThunk(this.client, USEAREA, params);
        if (areas.rows && areas.rows.length > 0) {
            const result: Record<string, any> = areas.rows[0];
            result.id = id;
            result.value = 0;
            return result;
        }
        const geostore: Record<string, any> = await GeostoreService.getGeostoreByUse(useTable, id, apiKey);
        if (geostore) {
            return {
                id,
                value: 0,
                area_ha: geostore.area_ha
            };
        }
        return null;
    }

    async getWdpa(wdpaid: string, period: string = defaultDate(), apiKey: string): Promise<Record<string, any> | void> {
        logger.debug('Obtaining wpda of id %s', wdpaid);
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            wdpaid,
            begin: periods[0],
            end: periods[1]
        };

        const data: Record<string, any> = await executeThunk(this.client, WDPA, params);
        if (data.rows && data.rows.length > 0) {
            const result: Record<string, any> = data.rows[0];
            result.id = wdpaid;
            result.period = period;
            result.downloadUrls = this.getDownloadUrls(WDPA, params);
            return result;
        }
        const areas: Record<string, any> = await executeThunk(this.client, WDPAAREA, params);
        if (areas.rows && areas.rows.length > 0) {
            const result: Record<string, any> = areas.rows[0];
            result.id = wdpaid;
            result.value = 0;
            return result;
        }
        const geostore: Record<string, any> = await GeostoreService.getGeostoreByWdpa(wdpaid, apiKey);
        if (geostore) {
            return {
                id: wdpaid,
                value: 0,
                area_ha: geostore.area_ha
            };
        }
        return null;
    }

    async getWorld(hashGeoStore: string, period: string = defaultDate(), apiKey: string): Promise<Record<string, any>> {
        logger.debug('Obtaining world with hashGeoStore %s', hashGeoStore);

        const geostore: Record<string, any> = await GeostoreService.getGeostoreByHash(hashGeoStore, apiKey);
        if (geostore && geostore.geojson) {
            return await this.getWorldWithGeojson(geostore.geojson, period);
        }
        throw new NotFound('Geostore not found');
    }

    async getWorldWithGeojson(geojson: Record<string, any>, period: string = defaultDate()): Promise<Record<string, any>> {
        logger.debug('Executing query in cartodb with geojson', geojson);
        const periods: string[] = period.split(',');
        const params: Record<string, any> = {
            geojson: JSON.stringify(geojson.features[0].geometry),
            begin: periods[0],
            end: periods[1]
        };
        const data: Record<string, any> = await executeThunk(this.client, WORLD, params);
        const dataArea: Record<string, any> = await executeThunk(this.client, AREA, params);
        const result: Record<string, any> = {
            area_ha: dataArea.rows[0].area_ha
        };
        if (data.rows) {
            result.value = data.rows[0].value || 0;

        }
        result.area_ha = dataArea.rows[0].area_ha;
        result.downloadUrls = this.getDownloadUrls(WORLD, params);
        return result;
    }

    async latest(): Promise<Array<Record<string, any>>> {
        logger.debug('Obtaining latest date');
        const data: Record<string, any> = await executeThunk(this.client, LATEST, {});
        if (data && data.rows && data.rows.length) {
            const result: Array<Record<string, any>> = data.rows;
            return result;
        }
        return null;
    }

}

export default new CartoDBServiceV2();
