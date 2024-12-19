import { PulsarConnection } from "./connection";

export class PulsarConnectionPool {
    private connections: PulsarConnection[] = [];
    maxActiveRequests = 500;
    maxConnections = 20;
    uri: URL;
    constructor(uri: URL, maxActiveRequests = 500, maxConnections = 20) {
        this.uri = uri;
        this.connections = [];
        this.maxConnections = maxConnections;
        this.maxActiveRequests = maxActiveRequests;
    }
    get = async() => {
        let connection = this.connections
            .sort((a, b) => a.getTotalRequests() - b.getTotalRequests())
            .reverse()
            .find(c => c.getTotalRequests() < this.maxActiveRequests);
        if (!connection) {
            if (this.connections.length >= this.maxConnections) {
                throw new Error("Max connections reached with max active requests");
            }
            connection = new PulsarConnection(this.uri);
            connection.on("end", () => {
                this.connections = this.connections.filter(c => c !== connection);
            });
            await connection.connect();
            this.connections.push(connection);
        }
        return connection;
    }
    close = () => {
        this.connections.forEach(c => c.close());
    }
}