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

var guiraLatestSerializer = new JSONAPISerializer('guira-latest', {
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
        return guiraLatestSerializer.serialize(data);
    }
}

module.exports = GuiraLossSerializerV2;
