'use strict';

var logger = require('logger');
var JSONAPISerializer = require('jsonapi-serializer').Serializer;
var guiraLossSerializerV2 = new JSONAPISerializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls', 'area_ha'],
    typeForAttribute: function(attribute, record) {
        return attribute;
    },
    downloadUrls: {
        attributes: ['csv', 'geojson', 'kml', 'shp', 'svg']
    },
    keyForAttribute: 'camelCase'
});

var guiraLatestSerializerV2 = new JSONAPISerializer('guira-latest', {
    attributes: ['latest'],
    typeForAttribute: function(attribute, record) {
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
