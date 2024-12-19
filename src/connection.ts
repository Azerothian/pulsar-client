import AsyncTCPClient from "./tcp-client";
import { BaseCommand, BaseCommandType, CommandLookupTopic, CommandLookupTopicResponse, CommandProducer } from "./types/pulsar/PulsarApi";
import protobuf from "protobufjs";
// import { crc32 } from "zlib";
import crc32 from "fast-crc32c"
// import path, { resolve } from "path";

import debug from "debug";
import { PulsarProducer } from "./producer";
import { EventEmitter } from "stream";
const logger =  {
    debug: debug("pulsar::connection::debug"),
    error: debug("pulsar::connection::error"),
};


export type PulsarMessage = {
    command: BaseCommand,
    payload: Buffer
}

const HEADERSIZE = 8;
const HDRCMDSIZE = 4;
const HDRTOTALSIZE = 4;

type RequestInfo = {
    rId: number,
    command: any,
    resolver: PromiseWithResolvers<any>,
    timeout?: NodeJS.Timeout
    resolved?: boolean
}


class UniqueIdMap<T> {
    private requests: Map<number, T> = new Map();
    private requestID = 1;
    getNextId = () => {
        // ... should never inf loop
        while (this.requests.has(this.requestID)) {
            this.requestID++;
            if (this.requestID > Number.MAX_SAFE_INTEGER) {
                this.requestID = 1;
            }
            if (!this.requests.has(this.requestID)) {
                return this.requestID;
            }
        }
    }
    add = (rId: number, req: T) => {
        this.requests.set(rId, req);
    }

    delete = (rId: number) => {
        this.requests.delete(rId);
    }
    get = (rId: number) => {
        return this.requests.get(rId);
    }
    length = () => {
        return this.requests.size;
    }
    getMap = () => {
        return this.requests;
    }
}


export class PulsarConnection extends EventEmitter {
    // private dataQueue: PulsarMessage[] = [];
    // private resolveQueue: ((data: PulsarMessage) => void)[] = [];

    private tcpClient: AsyncTCPClient;
    private protob: protobuf.Root;
    private baseCommand: protobuf.Type;

    private maxMessageSize = 5242880;
    private msKeepAlivePeriod = 30 * 1000; // 30 seconds
    private msRequestTimeout = 10000; // 10 seconds
    private keepAliveInterval: NodeJS.Timeout;

    private requests: UniqueIdMap<RequestInfo> = new UniqueIdMap<RequestInfo>();
    private producers: UniqueIdMap<PulsarProducer> = new UniqueIdMap<PulsarProducer>();

    uri: URL;
    constructor(uri: URL) {
        super();
        this.uri = uri;
        this.tcpClient = new AsyncTCPClient();
        this.tcpClient.on("end", this.handleTCPClientEnd);
    }
    getTotalRequests = () => {
        return this.requests.length();
    }
    

    handleTCPClientEnd = () => {
        clearInterval(this.keepAliveInterval);
        this.emit("end");
    }
    sendKeepAlive = () => {
        this.send({
            type: BaseCommandType.PING,
            ping: {}
        });
    }
    
    connect = async() => {
        this.baseCommand = this.protob.lookupType("BaseCommand");
        this.tcpClient.connect(this.uri.host, parseInt(this.uri.port || "6650"));
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
                    this.processProducerClose(command);
                    break;
                case BaseCommandType.PRODUCER_SUCCESS:
                    this.processProducerSuccess(command);
                    break;
                
                case BaseCommandType.LOOKUP_RESPONSE:
                    this.processLookupResponse(command);
                case BaseCommandType.ERROR:
                    break;
                default:
                    // this.handleIncomingData(command, payload);
                    break;
            }

            totalSize = 0;
            commandSize = 0;
            packetData = undefined;
            largePayload = false;
            currentIndex = 0;
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

    // sendMessage = (command: BaseCommand, payload?: Buffer) => {
    //     logger.debug("send", BaseCommandType[command.type]);
    //     const commandData = Buffer.from(this.baseCommand.encode(command).finish());
    //     const commandSize = commandData.byteLength;
    //     let frameSize = 4 + commandSize;
    //     if(payload) {
    //         frameSize += payload.byteLength;
    //     }
    //     let packet = Buffer.allocUnsafe(frameSize + 4);
    //     packet.writeUInt32BE(frameSize, 0);
    //     packet.writeUInt32BE(commandSize, 4);
    //     commandData.copy(packet, HEADERSIZE, 0, commandData.length)
    //     if (payload) {
    //         payload.copy(packet, HEADERSIZE + commandSize, 0, payload.length);
    //     }

    //     this.tcpClient.send(packet);
    // }


    close = () => {
        this.tcpClient.close();
        this.producers.getMap().forEach((producer) => {
            producer.close();
        });
        this.requests.getMap().forEach((req) => {
            clearTimeout(req.timeout);
            req.resolver.reject(new Error("Connection closed"));
        });
    }
    createRequest = (rId: number, command: any, resolver: PromiseWithResolvers<any>, hasTimeout = false, deleteOnSuccess = false): RequestInfo => {
        const req: RequestInfo = {
            rId,
            command,
            resolver,
            timeout: hasTimeout ? setTimeout(() => {
                // this is for if the command is an active component not a one off
                if (deleteOnSuccess) {
                    this.requests.delete(rId);
                }
                logger.error("Timeout", {
                    commandType: BaseCommandType[command.type],
                    command,
                    rId,
                });
                if (!req.resolved) {
                    req.resolved = true;
                    req.resolver.reject(new Error(`Timeout waiting for response for ${BaseCommandType[command.type]} request ${rId}`));
                }
            }, this.msRequestTimeout) : undefined
        };
        this.requests.add(rId, req);
        return req;
    }
    createProducer = (topic: string, producerName?: string): Promise<PulsarProducer> => {
        const rId = this.requests.getNextId();
        const producerId = this.producers.getNextId();

        let pName = producerName || `producer-${producerId}`;
        const cmdProducer: CommandProducer = {
            metadata: [],
            producerId: producerId,
            requestId: rId,
            topic: topic,
            producerName: pName,
            userProvidedProducerName: !!producerName,
        };
        const req = this.createRequest(rId, cmdProducer, Promise.withResolvers<PulsarProducer>(), true, true);
        this.send({
            type: BaseCommandType.PRODUCER,
            producer: cmdProducer
        });
        return req.resolver.promise;
    }
    sendProducer = (producer: PulsarProducer, sequenceId: number, payload: Buffer) => {
        this.send({
            type: BaseCommandType.SEND,
            send: {
                producerId: producer.producerId,
                sequenceId: sequenceId,
                numMessages: 1,
            }
        }, payload);
    }
    private processProducerSuccess = (command: BaseCommand) => {
        const rId = command.producer.requestId;
        const req = this.requests.get(rId);
        clearTimeout(req?.timeout);
        if (!req || req?.resolved) {
            return;
        }
        const producer = new PulsarProducer(this, command.producer);
        producer.on("end", () => {
            this.requests.delete(command.producer.requestId);
            this.producers.delete(command.producer.producerId);
        });
        req.resolved = true;
        req.resolver.resolve(producer);
        return true;
    }
    private processProducerClose = (command: BaseCommand) => {
        const producer = this.producers.get(command.closeProducer.producerId);
        if (producer) {
            producer.close();
        }
    }

    lookupTopic = async(topic: string, authoritative: boolean = false) : Promise<CommandLookupTopicResponse> => {
        const rId = this.requests.getNextId();
        const req = this.createRequest(rId, topic, Promise.withResolvers<CommandLookupTopicResponse>(), true, true);
        this.requests.add(rId, req);
        this.send({
            type: BaseCommandType.LOOKUP,
            lookupTopic: {
                requestId: rId,
                topic,
                authoritative,
                properties: [],
                advertisedListenerName: undefined,
            }
        });
        return req.resolver.promise;
    }
    private processLookupResponse = (command: BaseCommand) => {
        const rId = command.lookupTopic.requestId;
        const req = this.requests[rId];
        if (!req) {
            return;
        }
        req.resolver.resolve(command.lookupTopic);
        this.requests.delete(rId);
    }

}
