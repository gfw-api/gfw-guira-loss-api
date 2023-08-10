import Router from 'koa-router';
import logger from 'logger';
import { Context, Next } from 'koa';
import CartoDBService from 'services/cartoDBService';
import NotFound from 'errors/notFound';
import GuiraLossSerializer from "serializers/guiraLoss.serializer";


const routerV1: Router = new Router({
    prefix: '/api/v2/guira-loss'
});

class guiraLossRouterV1 {

    static async getNational(ctx: Context): Promise<void> {
        logger.info('Obtaining national data');
        const data: Record<string, any> | void = await CartoDBService.getNational(
            ctx.params.iso,
            ctx.query.period as string,
            ctx.request.headers['x-api-key'] as string
        );

        // TODO: null values should be better handled
        ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
    }

    static async getSubnational(ctx: Context): Promise<void> {
        logger.info('Obtaining subnational data');
        const data: Record<string, any> | void = await CartoDBService.getSubnational(
            ctx.params.iso,
            ctx.params.id1,
            ctx.query.period as string,
            ctx.request.headers['x-api-key'] as string
        );

        // TODO: null values should be better handled
        ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
    }

    static async use(ctx: Context): Promise<void> {
        logger.info('Obtaining use data with name %s and id %s', ctx.params.name, ctx.params.id);
        let useTable: string;
        switch (ctx.params.name) {

            case 'mining':
                useTable = 'gfw_mining';
                break;
            case 'oilpalm':
                useTable = 'gfw_oil_palm';
                break;
            case 'fiber':
                useTable = 'gfw_wood_fiber';
                break;
            case 'logging':
                useTable = 'gfw_logging';
                break;
            default:
                useTable = ctx.params.name;

        }
        if (!useTable) {
            ctx.throw(404, 'Name not found');
        }
        const data: Record<string, any> | void = await CartoDBService.getUse(
            ctx.params.name,
            useTable,
            ctx.params.id,
            ctx.query.period as string,
            ctx.request.headers['x-api-key'] as string
        );

        // TODO: null values should be better handled
        ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);

    }

    static async wdpa(ctx: Context): Promise<void> {
        logger.info('Obtaining wpda data with id %s', ctx.params.id);
        const data: Record<string, any> | void = await CartoDBService.getWdpa(
            ctx.params.id,
            ctx.query.period as string,
            ctx.request.headers['x-api-key'] as string
        );

        // TODO: null values should be better handled
        ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
    }

    static async world(ctx: Context): Promise<void> {
        logger.info('Obtaining world data');
        ctx.assert(ctx.query.geostore, 400, 'GeoJSON param required');
        try {
            const data: Record<string, any> | void = await CartoDBService.getWorld(
                ctx.query.geostore as string,
                ctx.query.period as string,
                ctx.request.headers['x-api-key'] as string
            );

            // TODO: null values should be better handled
            ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
        } catch (err) {
            if (err instanceof NotFound) {
                ctx.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static checkGeojson(geojson: Record<string, any>): Record<string, any> {
        if (geojson.type.toLowerCase() === 'polygon') {
            return {
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: geojson
                }]
            };
        }
        if (geojson.type.toLowerCase() === 'feature') {
            return {
                type: 'FeatureCollection',
                features: [geojson]
            };
        }
        return geojson;
    }

    static async worldWithGeojson(ctx: Context): Promise<void> {
        logger.info('Obtaining world data with geostore');
        ctx.assert((ctx.request.body as Record<string, any>).geojson, 400, 'GeoJSON param required');
        try {
            const data: Record<string, any> | void = await CartoDBService.getWorldWithGeojson(
                guiraLossRouterV1.checkGeojson((ctx.request.body as Record<string, any>).geojson),
                ctx.query.period as string
            );

            // TODO: null values should be better handled
            ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
        } catch (err) {
            if (err instanceof NotFound) {
                ctx.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static async latest(ctx: Context): Promise<void> {
        logger.info('Obtaining latest data');
        const data: void | any[] = await CartoDBService.latest(ctx.query.limit as string);

        // TODO: null values should be better handled
        ctx.response.body = GuiraLossSerializer.serialize(data as Record<string, any>);
    }

}

const isCached = async (ctx: Context, next: Next): Promise<void> => {
    if (await ctx.cashed()) {
        return;
    }
    await next();
};


routerV1.get('/admin/:iso', isCached, guiraLossRouterV1.getNational);
routerV1.get('/admin/:iso/:id1', isCached, guiraLossRouterV1.getSubnational);
routerV1.get('/use/:name/:id', isCached, guiraLossRouterV1.use);
routerV1.get('/wdpa/:id', isCached, guiraLossRouterV1.wdpa);
routerV1.get('/', isCached, guiraLossRouterV1.world);
routerV1.post('/', isCached, guiraLossRouterV1.worldWithGeojson);
routerV1.get('/latest', isCached, guiraLossRouterV1.latest);


export default routerV1;
