import PulsarClient from ".";
import debug from "debug";
debug.enable("*");

// (async () => {
//     const pulsar = new PulsarConnection();
//     pulsar.connect('10.43.215.210', 6650);
//     // do {
//     //     await sleep(100);
//     // } while(true)
// })();

(async() => {
    let client = new PulsarClient();
    await client.initialise("pulsar://10.43.215.210:6650");
    const connection = await client.getConnectionPool().get();
    console.log(connection);
    
    await sleep(60000);
    // let producer = await client.createProducer("my-topic");
    
})();

function sleep(ms = 100) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}134