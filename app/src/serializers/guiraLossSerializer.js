'use strict';

var logger = require('logger');
var JSONAPISerializer = require('jsonapi-serializer').Serializer;
var guiraLossSerializer = new JSONAPISerializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls'],
    typeForAttribute: function(attribute, record) {
        return attribute;
    },
    downloadUrls: {
        attributes: ['csv', 'geojson', 'kml', 'shp', 'svg']
    }
});

var guiraLatestSerializer = new JSONAPISerializer('guira-latest', {
    attributes: ['date'],
    typeForAttribute: function(attribute, record) {
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
