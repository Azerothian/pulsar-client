import AsyncTCPClient from "./utils/tcp-client";
import { BaseCommand, BaseCommandType, CommandCloseProducer, CommandLookupTopic, CommandLookupTopicResponse, CommandProducer, CommandSend, CommandSendReceipt, CommandSuccess, MessageMetadata } from "./types/pulsar/PulsarApi";
import protobuf from "protobufjs";
import crc32 from "fast-crc32c"
// import path, { resolve } from "path";

import debug from "debug";
import { PulsarProducer } from "./producer";
import { EventEmitter } from "stream";
import { createWriteStream } from "fs";
import { deflateAsync } from "./utils/zlib";
import PulsarClient from ".";
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

const magicCrc32c = 0x0e01;
const magicBrokerEntryMetadata = 0x0e02;

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
    private tcpClient: AsyncTCPClient;
    // private baseCommand: protobuf.Type;

    private maxMessageSize = 5242880;
    private msKeepAlivePeriod = 30 * 1000; // 30 seconds
    private msRequestTimeout = 10000; // 10 seconds
    private keepAliveInterval: NodeJS.Timeout;

    private requests: UniqueIdMap<RequestInfo> = new UniqueIdMap<RequestInfo>();
    private producers: UniqueIdMap<PulsarProducer> = new UniqueIdMap<PulsarProducer>();
    private pulsarClient: PulsarClient;
    private sequenceId = 0;
    private uri: URL;
    constructor(uri: URL, pulsarClient: PulsarClient) {
        super();
        this.uri = uri;
        this.pulsarClient = pulsarClient;
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

        this.tcpClient.connect(this.uri.hostname, parseInt(this.uri.port || "6650"));
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
            const command = this.pulsarClient.getProtobType("BaseCommand").decode(commandData) as unknown as BaseCommand;
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
                case BaseCommandType.SUCCESS:
                    this.processSuccessResponse(command);
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
    // [TOTAL_SIZE] [CMD_SIZE][CMD]
    // send = (command: BaseCommand, payload?: Buffer) => {
    //     logger.debug("send", BaseCommandType[command.type]);
    //     const commandData = Buffer.from(this.pulsarClient.getProtobType("BaseCommand").encode(command).finish());
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
    // Wire format
	// [TOTAL_SIZE] [CMD_SIZE][CMD] // [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
    send = async(command: BaseCommand, payload?: Buffer, metadataMessage?: MessageMetadata, doCompress = false) => {
        // const ws = createWriteStream()
        logger.debug("send", BaseCommandType[command.type]);
        const hasPayload = payload && payload.byteLength > 0 && metadataMessage;
        let frameSize = 4; // command size (do not include frame header size)
        const commandData = Buffer.from(this.pulsarClient.getProtobType("BaseCommand").encode(command).finish());
        frameSize += commandData.byteLength;;
        let processedPayload, metadataSize, metadata;
        if (hasPayload) {
            frameSize += 4; // MAGIC_NUMBER
            frameSize += 4; // CHECKSUM
            frameSize += 4; // METADATA_SIZE

            metadata = Buffer.from(this.pulsarClient.getProtobType("MessageMetadata").encode(metadataMessage).finish());
            metadataSize = metadata.byteLength;
            frameSize += metadataSize;

            let processedPayload = payload;
            if (doCompress) {
                processedPayload = await deflateAsync(payload);
            }

            // TODO: encrypt payload ?
            frameSize += processedPayload.byteLength; // Remaining size


        }
        const packet = Buffer.allocUnsafe(frameSize + 4); // frame size does not include the frame size itself
        packet.writeUInt32BE(frameSize, 0); // total size
        packet.writeUInt32BE(commandData.byteLength, 4); // command size
        commandData.copy(packet, HEADERSIZE, 0, commandData.length); // command

        if (hasPayload) {
            const crcBufferSize = 4 + metadataSize + processedPayload.byteLength;
            
            const crcBuffer = Buffer.allocUnsafe(crcBufferSize);
            crcBuffer.writeUInt32BE(metadataSize, 0);
            metadata.copy(crcBuffer, 4, 0, metadataSize);
            processedPayload.copy(crcBuffer, 4 + metadataSize, 0, processedPayload.byteLength);
            const crc = crc32.calculate(crcBuffer);

            packet.writeUInt32BE(magicCrc32c, HEADERSIZE + commandData.length); // magic number for Crc32c
            packet.writeUInt32BE(crc, HEADERSIZE + commandData.length + 4); // crc
            packet.writeUInt32BE(metadataSize, HEADERSIZE + commandData.length + 8); // metadata size
            metadata.copy(packet, HEADERSIZE + commandData.length + 12, 0, metadataSize); // metadata
            processedPayload.copy(packet, HEADERSIZE + commandData.length + 12 + metadataSize, 0, processedPayload.byteLength); // payload
        }
        return this.tcpClient.send(packet);
    }
    sendMessage = async(payload: Buffer, producer: PulsarProducer, waitForReceipt = false) : Promise<CommandSendReceipt | undefined> => {
        logger.debug("send message");
        const rId = this.requests.getNextId();
        const sequenceId = this.sequenceId++;
        const cmdSend: CommandSend = {
            producerId: producer.producerId,
            sequenceId: sequenceId,
            numMessages: 1,
        };

        const metadataMessage: MessageMetadata = {
            producerName: producer.producerName,
            sequenceId: sequenceId,
            // numMessagesInBatch: 1,
            uncompressedSize: payload.byteLength,
            // partitionKey: "",
            properties: [],
            // eventTime: 0,
            // partitionKeyB64Encoded: false,
            // replicatedFrom: "",
            publishTime: Date.now(),
            replicateTo: [],
            encryptionKeys: [],
        };
        const req = this.createRequest(rId, cmdSend, Promise.withResolvers<any>(), true, true);
        this.send({
            type: BaseCommandType.SEND,
            send: cmdSend
        }, payload, metadataMessage, true);
        if (waitForReceipt) {
            return req.resolver.promise
        }
        return undefined;
    }


    close = async() => {
        logger.debug("close");
        this.tcpClient.close();
        await Promise.all(this.producers.getMap().values().map((producer) => {
            return producer.close(true);
        }));
        this.requests.getMap().forEach((req) => {
            clearTimeout(req.timeout);
            req.resolver.reject(new Error("Connection closed"));
        });
    }

    createRequest = (rId: number, command: any, resolver: PromiseWithResolvers<any>, hasTimeout = false, deleteOnSuccess = false): RequestInfo => {
        logger.debug("createRequest", command);
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
    createProducer = async (topic: string, producerName?: string): Promise<PulsarProducer> => {
        logger.debug("createProducer", topic, producerName);
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
        await this.send({
            type: BaseCommandType.PRODUCER,
            producer: cmdProducer
        });
        return req.resolver.promise;
    }
    closeProducer = async(producer: PulsarProducer) => {
        logger.debug("closeProducer", producer);
        const rId = this.requests.getNextId(); 
        const cmdCloseProducer: CommandCloseProducer = {
            producerId: producer.producerId,
            requestId: rId,
        };
        const req = this.createRequest(rId, cmdCloseProducer, Promise.withResolvers<CommandSuccess>(), true, true);
        await this.send({
            type: BaseCommandType.CLOSE_PRODUCER,
            closeProducer: cmdCloseProducer,
        });
        return req.resolver.promise;
    }

    private processSuccessResponse = (command: BaseCommand) => {
        logger.debug("processSuccessResponse", BaseCommandType[command.type]);
        this.resolveRequest(command.success.requestId, command.success);
    }

    private processProducerSuccess = (command: BaseCommand) => {
        logger.debug("processProducerSuccess", command);
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
        logger.debug("processProducerClose", command);
        const producer = this.producers.get(command.closeProducer.producerId);
        if (producer) {
            producer.close(true);
        }
    }

    lookupTopic = async(topic: string, authoritative: boolean = false) : Promise<CommandLookupTopicResponse> => {
        logger.debug("lookupTopic", topic, authoritative);
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
        logger.debug("processLookupResponse", command);
        this.resolveRequest(command.lookupTopicResponse.requestId, command.lookupTopicResponse);
        
    }
    private resolveRequest = (rId: number, command: any) => {
        const req = this.requests[rId];
        if (!req) {
            return;
        }
        req.resolver.resolve(command);
        this.requests.delete(rId);
    }

}
