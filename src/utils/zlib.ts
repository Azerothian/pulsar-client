import zlib from "node:zlib";
// import { pipeline } from "node:stream/promises";
import { promisify } from "node:util";

export const deflateAsync = promisify(zlib.deflate);


