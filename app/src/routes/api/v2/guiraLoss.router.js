
const Router = require('koa-router');
const logger = require('logger');
const CartoDBServiceV2 = require('services/cartoDBServiceV2');
const NotFound = require('errors/notFound');
const GuiraLossSerializerV2 = require('serializers/guiraLossSerializerV2');


const router = new Router({
    prefix: '/guira-loss'
});

class guiraLossRouter {

    static* getAdm0() {
        logger.info('Obtaining national data');
        const data = yield CartoDBServiceV2.getAdm0(this.params.iso, this.query.period);

        this.body = GuiraLossSerializerV2.serialize(data);
    }

    static* getAdm1() {
        logger.info('Obtaining subnational data');
        const data = yield CartoDBServiceV2.getAdm1(this.params.iso, this.params.id1, this.query.period);
        this.body = GuiraLossSerializerV2.serialize(data);
    }

    static* getAdm2() {
        logger.info('Obtaining subnational data');
        const data = yield CartoDBServiceV2.getAdm2(this.params.iso, this.params.id1, this.params.id2, this.query.period);
        this.body = GuiraLossSerializerV2.serialize(data);
    }

    static* use() {
        logger.info('Obtaining use data with name %s and id %s', this.params.name, this.params.id);
        const useTable = this.params.name;
        if (!useTable) {
            this.throw(404, 'Name not found');
        }
        const data = yield CartoDBServiceV2.getUse(useTable, this.params.id, this.query.period);
        this.body = GuiraLossSerializerV2.serialize(data);

    }

    static* wdpa() {
        logger.info('Obtaining wpda data with id %s', this.params.id);
        const data = yield CartoDBServiceV2.getWdpa(this.params.id, this.query.period);
        this.body = GuiraLossSerializerV2.serialize(data);
    }

    static* world() {
        logger.info('Obtaining world data');
        this.assert(this.query.geostore, 400, 'GeoJSON param required');
        try {
            const data = yield CartoDBServiceV2.getWorld(this.query.geostore, this.query.period);
            this.body = GuiraLossSerializerV2.serialize(data);
        } catch (err) {
            if (err instanceof NotFound) {
                this.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static checkGeojson(geojson) {
        if (geojson.type.toLowerCase() === 'polygon') {
            return {
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: geojson
                }]
            };
        } if (geojson.type.toLowerCase() === 'feature') {
            return {
                type: 'FeatureCollection',
                features: [geojson]
            };
        }
        return geojson;
    }

    static* worldWithGeojson() {
        logger.info('Obtaining world data with geostore');
        this.assert(this.request.body.geojson, 400, 'GeoJSON param required');
        try {
            const data = yield CartoDBServiceV2.getWorldWithGeojson(guiraLossRouter.checkGeojson(this.request.body.geojson), null, this.query.period);

            this.body = GuiraLossSerializerV2.serialize(data);
        } catch (err) {
            if (err instanceof NotFound) {
                this.throw(404, 'Geostore not found');
                return;
            }
            throw err;
        }

    }

    static* latest() {
        logger.info('Obtaining latest data');
        const data = yield CartoDBServiceV2.latest(this.query.limit);
        this.body = GuiraLossSerializerV2.serializeLatest(data);
    }

}

const isCached = function* isCached(next) {
    if (yield this.cashed()) {
        return;
    }
    yield next;
};


router.get('/admin/:iso', isCached, guiraLossRouter.getAdm0);
router.get('/admin/:iso/:id1', isCached, guiraLossRouter.getAdm1);
router.get('/admin/:iso/:id1/:id2', isCached, guiraLossRouter.getAdm2);
router.get('/use/:name/:id', isCached, guiraLossRouter.use);
router.get('/wdpa/:id', isCached, guiraLossRouter.wdpa);
router.get('/', isCached, guiraLossRouter.world);
router.post('/', isCached, guiraLossRouter.worldWithGeojson);
router.get('/latest', isCached, guiraLossRouter.latest);


module.exports = router;
