const JSONAPISerializer = require('jsonapi-serializer').Serializer;

const guiraLossSerializerV2 = new JSONAPISerializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls', 'area_ha'],
    typeForAttribute(attribute) {
        return attribute;
    },
    downloadUrls: {
        attributes: ['csv', 'json', 'kml', 'shp', 'svg']
    },
    keyForAttribute: 'camelCase'
});

const guiraLatestSerializerV2 = new JSONAPISerializer('guira-latest', {
    attributes: ['latest'],
    typeForAttribute(attribute) {
        return attribute;
    }
});

class GuiraLossSerializerV2 {

    static serialize(data) {
        return guiraLossSerializerV2.serialize(data);
    }

    static serializeLatest(data) {
        return guiraLatestSerializerV2.serialize(data);
    }

}

module.exports = GuiraLossSerializerV2;
