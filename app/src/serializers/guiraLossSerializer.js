const JSONAPISerializer = require('jsonapi-serializer').Serializer;

const guiraLossSerializer = new JSONAPISerializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls', 'area_ha'],
    typeForAttribute(attribute) {
        return attribute;
    },
    downloadUrls: {
        attributes: ['csv', 'geojson', 'kml', 'shp', 'svg']
    },
    keyForAttribute: 'camelCase'
});

const guiraLatestSerializer = new JSONAPISerializer('guira-latest', {
    attributes: ['date'],
    typeForAttribute(attribute) {
        return attribute;
    }
});

class GuiraLossSerializer {

    static serialize(data) {
        return guiraLossSerializer.serialize(data);
    }

    static serializeLatest(data) {
        return guiraLatestSerializer.serialize(data);
    }

}

module.exports = GuiraLossSerializer;
