import { PulsarConnection } from "./connection";
import { CommandProducer } from "./types/pulsar/PulsarApi";

export class PulsarProducer {
    
    private dataQueue: Buffer[] = [];
    private resolveQueue: ((data: Buffer) => void)[] = [];
    private connection: PulsarConnection;
    
    topic: string;
    producerName: string;
    private producerId: number;
    
    constructor(connection: PulsarConnection, commandProducer: CommandProducer) {
        this.connection = connection;
        this.topic = commandProducer.topic;
        this.producerName = commandProducer.producerName;
        this.producerId = commandProducer.producerId;
    }
    send = (message: Buffer, waitForAck = false) => {
        
    }
    close = () => {
        // close producer
    }
    
    async *[Symbol.asyncIterator]() {
        while (true) {
            const data = await new Promise<Buffer>((resolve) => {
                if (this.dataQueue.length > 0) {
                    // Immediate resolve if there's already data
                    resolve(this.dataQueue.shift()!);
                } else {
                    // Otherwise, queue the resolver
                    this.resolveQueue.push(resolve);
                }
            });
            yield data;
        }
    }
}