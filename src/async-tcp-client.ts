import * as net from 'net';
import { EventEmitter } from 'events';

export default class AsyncTCPClient extends EventEmitter {
    private socket: net.Socket;
    private dataQueue: Buffer[] = [];
    private resolveQueue: ((data: Buffer) => void)[] = [];

    connect = (host: string, port: number) => {
        this.socket = net.createConnection({ host, port });
        this.socket.on('data', (data) =>{
            // console.log("data", data);
            this.handleIncomingData(data)
        });
        this.socket.on('connect', () => this.emit("connect"));
        this.socket.on("ready", () => this.emit("ready"));
        this.socket.on("error", (e: any) => {
            console.error("error", e);
            this.emit("error", e)
        });
        this.socket.on("end", () => {
            console.log("end");
            this.emit("end")
        })
        // this.socket.connect({
        //     host,
        //     port,
        // })
    }

    private handleIncomingData(data: Buffer) {
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

    send = (data: string | Buffer) => {
        this.socket.write(data);
    }

    close = () => {
        this.socket.end();
    }
}
