import { Serializer } from 'jsonapi-serializer';

const guiraLossSerializer: Serializer = new Serializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls', 'area_ha'],
    typeForAttribute: (attribute: string) => attribute,
    downloadUrls: {
        attributes: ['csv', 'geojson', 'kml', 'shp', 'svg']
    },
    keyForAttribute: 'camelCase'
});

const guiraLatestSerializer: Serializer = new Serializer('guira-latest', {
    attributes: ['date'],
    typeForAttribute: (attribute: string) => attribute,
});

export default class GuiraLossSerializer {

    static serialize(data: Record<string, any>): Record<string, any> {
        return guiraLossSerializer.serialize(data);
    }

    static serializeLatest(data: Record<string, any>): Record<string, any> {
        return guiraLatestSerializer.serialize(data);
    }

}

module.exports = GuiraLossSerializer;
