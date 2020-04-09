const config = require('config');
const logger = require('logger');
const path = require('path');
const koa = require('koa');
const bodyParser = require('koa-bodyparser');
const koaLogger = require('koa-logger');
const loader = require('loader');
const validate = require('koa-validate');
const ErrorSerializer = require('serializers/errorSerializer');
const convert = require('koa-convert');
const koaSimpleHealthCheck = require('koa-simple-healthcheck');

// instance of koa
const app = koa();

// if environment is dev then load koa-logger
if (process.env.NODE_ENV === 'dev') {
    app.use(koaLogger());
}

app.use(bodyParser({
    jsonLimit: '50mb'
}));

// catch errors and send in jsonapi standard. Always return vnd.api+json
app.use(function* handleErrors(next) {
    try {
        yield next;
    } catch (inErr) {
        let error = inErr;
        try {
            error = JSON.parse(inErr);
        } catch (e) {
            logger.debug('Could not parse error message - is it JSON?: ', inErr);
            error = inErr;
        }
        this.status = error.status || this.status || 500;
        if (this.status >= 500) {
            logger.error(error);
        } else {
            logger.info(error);
        }

        this.body = ErrorSerializer.serializeError(this.status, error.message);
        if (process.env.NODE_ENV === 'prod' && this.status === 500) {
            this.body = 'Unexpected error';
        }
    }
    this.response.type = 'application/vnd.api+json';
});

app.use(convert.back(koaSimpleHealthCheck()));

const cache = require('lru-cache')({
    maxAge: 30000 // global max age
});

app.use(require('koa-cash')({
    get(key) {
        logger.debug('Getting the cache key: %s', key);
        return cache.get(key);
    },
    set(key, value) {
        logger.debug('Setting in cache. key: %s, value: ', key, value);
        cache.set(key, value);
    }
}));

// load custom validator
app.use(validate());

// load routes
loader.loadRoutes(app);

// Instance of http module
const appServer = require('http').Server(app.callback());

// get port of environment, if not exist obtain of the config.
// In production environment, the port must be declared in environment variable
const port = process.env.PORT || config.get('service.port');

const server = appServer.listen(port, () => {
    const microserviceClient = require('vizz.microservice-client');

    microserviceClient.register({
        id: config.get('service.id'),
        name: config.get('service.name'),
        dirConfig: path.join(__dirname, '../microservice'),
        dirPackage: path.join(__dirname, '../../'),
        logger,
        app
    });
    if (process.env.CT_REGISTER_MODE && process.env.CT_REGISTER_MODE === 'auto') {
        microserviceClient.autoDiscovery(config.get('service.name'));
    }
});
logger.info(`Server started in port:${port}`);

module.exports = server;
