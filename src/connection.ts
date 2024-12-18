import AsyncTCPClient from "./async-tcp-client";
import { BaseCommand, BaseCommandType, CommandProducer } from "./types/pulsar/PulsarApi";
import protobuf from "protobufjs";
import path, { resolve } from "path";

import debug from "debug";
import { PulsarProducer } from "./producer";
const logger =  {
    debug: debug("pulsar::connection::debug")
};


export type PulsarMessage = {
    command: BaseCommand,
    payload: Buffer
}

const HEADERSIZE = 8;
const HDRCMDSIZE = 4;
const HDRTOTALSIZE = 4;

export class PulsarConnection {
    private dataQueue: PulsarMessage[] = [];
    private resolveQueue: ((data: PulsarMessage) => void)[] = [];

    private tcpClient: AsyncTCPClient;
    private protob: protobuf.Root;
    private baseCommand: protobuf.Type;

    private maxMessageSize = 5242880;
    private msKeepAlivePeriod = 30 * 1000; // 30 seconds
    private keepAliveInterval: NodeJS.Timeout;

    private requestID = 1;
    private requests: {rId: number, command: any, resolver: PromiseWithResolvers<any> }[] = [];
    
    private producerIdx = 1;
    private producers: Map<number, PulsarProducer> = new Map();

    constructor() {
        this.tcpClient = new AsyncTCPClient();
    }
    sendKeepAlive = () => {
        this.send({
            type: BaseCommandType.PING,
            ping: {}
        });
    }
    
    connect = async(host: string, port: number) => {
        this.protob = await protobuf.load(path
            .resolve(__dirname, "../proto/PulsarApi.proto"));
        this.baseCommand = this.protob.lookupType("BaseCommand");
        this.tcpClient.connect(host, port);
        this.tcpClient.once("ready", () => {
            const command: BaseCommand = {
                type: BaseCommandType.CONNECT,

                connect: {
                    clientVersion: "Pure NodeJS Client 0.0.1",
                    protocolVersion: 21,
                    // authMethodName: ,

                    // authMethod: AuthMethod.AuthMethodNone,
                    // featureFlags: {
                    //     supportsAuthRefresh: false,
                    //     supportsBrokerEntryMetadata: false,
                    // }
                }
            };
            this.send(command);
        });

        let totalSize = 0;
        let commandSize = 0;
        let packetData: Buffer = undefined;
        // let payload: Buffer = undefined;
        let largePayload = false;
        let currentIndex = 0;

        for await (let data of this.tcpClient) {
            if (!largePayload) {
                totalSize = data.readUInt32BE(0);
                commandSize = data.readUInt32BE(4);
                currentIndex = 0;

                if (totalSize + HDRTOTALSIZE > data.length) {
                    largePayload = true;
                }
                packetData = Buffer.allocUnsafe(totalSize + HDRTOTALSIZE);
            }
            data.copy(packetData, currentIndex, 0, data.length);

            if (largePayload) {
                currentIndex += data.length;
                if (currentIndex <= totalSize + HDRTOTALSIZE) {
                    continue;
                }
                largePayload = false;
            }

            const commandData = Buffer.copyBytesFrom(packetData, HEADERSIZE, commandSize);
            const command = this.baseCommand.decode(commandData) as unknown as BaseCommand;
            logger.debug("received", BaseCommandType[command.type]);

            let payload: Buffer = undefined
            let payloadSize = (totalSize - HDRCMDSIZE - commandSize) // totalSize - commandSizeHeader - commandSize
            if (payloadSize > 0) {
                // PayLoad
                const payloadStartIndex = (HEADERSIZE + commandSize);
                payload = Buffer.copyBytesFrom(packetData, payloadStartIndex, payloadSize);

            }

            switch(command.type) {
                case BaseCommandType.CONNECTED:
                    this.maxMessageSize = command.connected.maxMessageSize;
                    this.keepAliveInterval = setInterval(this.sendKeepAlive, this.msKeepAlivePeriod);
                    break;
                case BaseCommandType.PING:
                    this.send({
                        type: BaseCommandType.PONG,
                        pong: {}
                    })
                    break
                case BaseCommandType.PONG:
                    // todo: kill connection if pong never received
                    break;
                case BaseCommandType.CLOSE_PRODUCER:
                    break;
                case BaseCommandType.PRODUCER_SUCCESS:
                    this.processProducerSuccess(command);

                    break;
                case BaseCommandType.ERROR:
                    break;
                default:
                    this.handleIncomingData(command, payload);
                    break;
            }

            totalSize = 0;
            commandSize = 0;
            packetData = undefined;
            largePayload = false;
            currentIndex = 0;
        }
    }

    
    private handleIncomingData(command: BaseCommand, payload: Buffer) {
        const data = {
            command,
            payload
        };
        if (this.resolveQueue.length > 0) {
            // Resolve the first pending promise with the data
            const resolve = this.resolveQueue.shift()!;
            resolve(data);
        } else {
            // Queue the data for future iteration
            this.dataQueue.push(data);
        }
    }
    async *[Symbol.asyncIterator]() {
        while (true) {
            const data = await new Promise<PulsarMessage>((resolve) => {
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

    send = (command: BaseCommand, payload?: Buffer) => {
        logger.debug("send", BaseCommandType[command.type]);
        const commandData = Buffer.from(this.baseCommand.encode(command).finish());
        const commandSize = commandData.byteLength;
        let frameSize = 4 + commandSize;
        if(payload) {
            frameSize += payload.byteLength;
        }
        let packet = Buffer.allocUnsafe(frameSize + 4);
        packet.writeUInt32BE(frameSize, 0);
        packet.writeUInt32BE(commandSize, 4);
        commandData.copy(packet, HEADERSIZE, 0, commandData.length)
        if (payload) {
            payload.copy(packet, HEADERSIZE + commandSize, 0, payload.length);
        }
        this.tcpClient.send(packet);
    }

    close = () => {
        clearInterval(this.keepAliveInterval);
        this.tcpClient.close();
    }
    createProducer = (topic: string, producerName?: string): Promise<PulsarProducer> => {
        // lookup and verifiy topic

        const rId = this.requestID++;
        const producerId = this.producerIdx++;

        let pName = producerName || `producer-${producerId}`;
        const cmdProducer: CommandProducer = {
            metadata: [],
            producerId: producerId,
            requestId: rId,
            topic: topic,
            producerName: pName,
            userProvidedProducerName: !!producerName,
        };
        const req = {
            rId,
            command: cmdProducer,
            resolver: Promise.withResolvers<PulsarProducer>()
        };
        this.requests.push(req);
        this.send({
            type: BaseCommandType.PRODUCER,
            producer: cmdProducer
        });
        return req.resolver.promise;
    }
    private processProducerSuccess = (command: BaseCommand) => {
        const rId = command.producer.requestId;
        const req = this.requests.find(r => r.rId === rId);
        if (!req) {
            return;
        }
        const producer = new PulsarProducer(this, command.producer);
        req.resolver.resolve(producer);
        return true;
    }

}


// export class PulsarConnectionPool {
//     private connections: PulsarConnection[] = [];
//     private currentIndex = 0;
//     constructor() {
//         this.connections = [];
//     }

//     addConnection = (connection: PulsarConnection) => {
//         this.connections.push(connection);
//     }

//     getConnection = () => {
//         if (this.currentIndex >= this.connections.length) {
//             this.currentIndex = 0;
//         }
//         return this.connections[this.currentIndex++];
//     }
// }




// (async () => {
//     const pulsar = new PulsarConnection();
//     pulsar.connect('10.43.215.210', 6650);
//     // do {
//     //     await sleep(100);
//     // } while(true)
// })();

// function sleep(ms = 100) {
//     return new Promise((resolve) => setTimeout(resolve, ms));
// }
