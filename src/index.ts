import { PulsarConnection } from "./connection"
import { CommandAckAckType, CommandProducer } from "./types/pulsar/PulsarApi";
function reqId() {
    return 'xxxyxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}






export default class PulsarClient {
    

    connection: PulsarConnection;
    constructor() {
        this.connection = new PulsarConnection();
    }
    connect = (host: string, port: number) => {
        return this.connection.connect(host, port);
    }
}