import { Serializer } from 'jsonapi-serializer';

const guiraLossSerializerV2: Serializer = new Serializer('guira-loss', {
    attributes: ['value', 'period', 'min_date', 'max_date', 'downloadUrls', 'area_ha'],
    typeForAttribute: (attribute: string) => attribute,
    downloadUrls: {
        attributes: ['csv', 'json', 'kml', 'shp', 'svg']
    },
    keyForAttribute: 'camelCase'
});

const guiraLatestSerializerV2: Serializer = new Serializer('guira-latest', {
    attributes: ['latest'],
    typeForAttribute: (attribute: string) => attribute,
});

export default class GuiraLossSerializerV2 {

    static serialize(data: Record<string, any>): Record<string, any> {
        return guiraLossSerializerV2.serialize(data);
    }

    static serializeLatest(data: Record<string, any>): Record<string, any> {
        return guiraLatestSerializerV2.serialize(data);
    }

}

module.exports = GuiraLossSerializerV2;
