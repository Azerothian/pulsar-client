import path from "path";
import { PulsarConnectionPool } from "./pool";
import protobuf from "protobufjs";
import { CommandLookupTopicResponseLookupType } from "./types/pulsar/PulsarApi";
// function reqId() {
//     return 'xxxyxxxx'.replace(/[xy]/g, function(c) {
//         var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
//         return v.toString(16);
//     });
// }






export default class PulsarClient {
    private uri: URL;
    private protob: protobuf.Root;
    private protobIndex: { [key: string]: protobuf.Type } = {};
    private pools: Map<URL, PulsarConnectionPool> = new Map();
    constructor() {
    }
    initialise = async(uri: string) => {
        this.uri = new URL(uri);
        this.protob = await protobuf.load(path.resolve(__dirname, "../proto/PulsarApi.proto"));
    }
    getProtobType = (type: string) => {
        let protobType = this.protobIndex[type];
        if (!protobType) {
            protobType = this.protob.lookupType(type);
            this.protobIndex[type] = protobType;
        }
        return protobType;
    }
    // getProtob = () => {
    //     return this.protob;
    // }
    lookupTopic = async(topic: string, uri: URL = this.uri, authoritative = false) => {
        const connection = await this.getConnectionPool(uri).get();
        const lookupTopic = await connection.lookupTopic(topic, authoritative);
        switch (lookupTopic.response) {
            case CommandLookupTopicResponseLookupType.Redirect:
                return this.lookupTopic(topic, new URL(lookupTopic.brokerServiceUrlTls || lookupTopic.brokerServiceUrl), lookupTopic.authoritative);
            case CommandLookupTopicResponseLookupType.Connect:
                return lookupTopic;
            case CommandLookupTopicResponseLookupType.Failed:
            case CommandLookupTopicResponseLookupType.UNRECOGNIZED:
                break;
        }
        return null;
    }
    createProducer = async(topic: string, producerName?: string) => {
        const lookupTopic = await this.lookupTopic(topic);
        if (!lookupTopic) {
            throw new Error("Failed to lookup topic");
        }
        const connection = await this.getConnectionPool(new URL(lookupTopic.brokerServiceUrlTls || lookupTopic.brokerServiceUrl)).get();
        const producer = await connection.createProducer(topic, producerName);
        return producer;
    }
    
    getConnectionPool = (uri?: URL) => {
        if (!uri) {
            uri = this.uri;
        }
        let pool = this.pools.get(uri);
        if (!pool) {
            pool = new PulsarConnectionPool(uri, this);
            this.pools.set(uri, pool);
        }
        return pool;

        
    }
}