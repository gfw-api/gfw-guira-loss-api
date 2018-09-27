'use strict';

var Router = require('koa-router');
var logger = require('logger');
var CartoDBService = require('services/cartoDBService');
var NotFound = require('errors/notFound');
var GuiraLossSerializer = require('serializers/guiraLossSerializer');


var router = new Router({
    prefix: '/guira-loss'
});

class guiraLossRouter {
    static * getNational() {
        logger.info('Obtaining national data');
        let data = yield CartoDBService.getNational(this.params.iso, this.query.period);

        this.body = GuiraLossSerializer.serialize(data);
    }

    static * getSubnational() {
        logger.info('Obtaining subnational data');
        let data = yield CartoDBService.getSubnational(this.params.iso, this.params.id1, this.query.period);
        this.body = GuiraLossSerializer.serialize(data);
    }

    static * use() {
        logger.info('Obtaining use data with name %s and id %s', this.params.name, this.params.id);
        let useTable = null;
        switch (this.params.name) {
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
                useTable = this.params.name;
        }
        if (!useTable) {
            this.throw(404, 'Name not found');
        }
        let data = yield CartoDBService.getUse(this.params.name, useTable, this.params.id, this.query.period);
        this.body = GuiraLossSerializer.serialize(data);

    }

    static * wdpa() {
        logger.info('Obtaining wpda data with id %s', this.params.id);
        let data = yield CartoDBService.getWdpa(this.params.id, this.query.period);
        this.body = GuiraLossSerializer.serialize(data);
    }

    static * world() {
        logger.info('Obtaining world data');
        this.assert(this.query.geostore, 400, 'GeoJSON param required');
        try {
            let data = yield CartoDBService.getWorld(this.query.geostore, this.query.period);
            this.body = GuiraLossSerializer.serialize(data);
        } catch (err) {
            if (err instanceof NotFound) {
                this.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static checkGeojson(geojson) {
        if (geojson.type.toLowerCase() === 'polygon'){
            return {
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: geojson
                }]
            };
        } else if (geojson.type.toLowerCase() === 'feature') {
            return {
                type: 'FeatureCollection',
                features: [geojson]
            };
        }
        return geojson;
    }

    static * worldWithGeojson() {
        logger.info('Obtaining world data with geostore');
        this.assert(this.request.body.geojson, 400, 'GeoJSON param required');
        try{
            let data = yield CartoDBService.getWorldWithGeojson(guiraLossRouter.checkGeojson(this.request.body.geojson), null, this.query.period);

            this.body = GuiraLossSerializer.serialize(data);
        } catch(err){
            if(err instanceof NotFound){
                this.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static * latest() {
        logger.info('Obtaining latest data');
        let data = yield CartoDBService.latest(this.query.limit);
        this.body = GuiraLossSerializer.serializeLatest(data);
    }

}

var isCached = function*(next) {
    if (yield this.cashed()) {
        return;
    }
    yield next;
};



router.get('/admin/:iso', isCached, guiraLossRouter.getNational);
router.get('/admin/:iso/:id1', isCached, guiraLossRouter.getSubnational);
router.get('/use/:name/:id', isCached, guiraLossRouter.use);
router.get('/wdpa/:id', isCached, guiraLossRouter.wdpa);
router.get('/', isCached, guiraLossRouter.world);
router.post('/', isCached, guiraLossRouter.worldWithGeojson);
router.get('/latest', isCached, guiraLossRouter.latest);


module.exports = router;
