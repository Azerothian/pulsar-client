import { EventEmitter } from "stream";
import { PulsarConnection } from "./connection";
import { CommandProducer } from "./types/pulsar/PulsarApi";

export class PulsarProducer extends EventEmitter {
    // private dataQueue: Buffer[] = [];
    // private resolveQueue: ((data: Buffer) => void)[] = [];
    private connection: PulsarConnection;
    
    topic: string;
    producerName: string;
    producerId: number;
    private closed = false;
    
    constructor(connection: PulsarConnection, commandProducer: CommandProducer) {
        super();
        this.connection = connection;
        this.topic = commandProducer.topic;
        this.producerName = commandProducer.producerName;
        this.producerId = commandProducer.producerId;
    }
    send = async(message: Buffer, waitForAck = false) => {
        
    }
    close = () => {
        // close producer
        this.closed = true;
        this.emit("end");
    }
    
    // async *[Symbol.asyncIterator]() {
    //     while (true) {
    //         const data = await new Promise<Buffer>((resolve) => {
    //             if (this.dataQueue.length > 0) {
    //                 // Immediate resolve if there's already data
    //                 resolve(this.dataQueue.shift()!);
    //             } else {
    //                 // Otherwise, queue the resolver
    //                 this.resolveQueue.push(resolve);
    //             }
    //         });
    //         yield data;
    //     }
    // }
}