import { PhotoBucket, PhotoDoc, ErrorCb, ResultCb, Db } from './PhotoBucket';
import { IncomingMessage } from 'http';
const random = require('meteor-random');
const bhttp = require('bhttp');
const krakenUrl = 'https://api.kraken.io/v1/upload';

export class OptimizingPhotoBucket extends PhotoBucket {
  constructor(db: Db, private auth: { api_secret: string, api_key: string }) {
    super(db);
  }

  optimize(doc: PhotoDoc, cb: ResultCb<string>): void {
    const newId = random.id();
    const newMetadata = {
      type: doc.metadata.type,
      isOptimized: true,
      checksum: doc.metadata.checksum,
      space: doc.metadata.space,
      height: doc.metadata.height,
      width: doc.metadata.width
    };

    const readStream = this.openDownloadStream(doc._id as any);
    const writeStream = this.openUploadStreamWithId(newId, doc.filename, {
      contentType: doc.contentType,
      metadata: newMetadata
    });
    this.sendToKraken(readStream, doc, writeStream, err => cb(err, newId));
  }

  private sendToKraken(upStream: NodeJS.ReadableStream, doc: PhotoDoc, downStream: NodeJS.WritableStream, cb: ErrorCb): void {
    const krakenOptions = {
      auth: this.auth,
      wait: true,
      lossy: true
    };

    let wrappedStream: any;
    try {
      const streamInfo = getStreamInfo(doc);
      wrappedStream = bhttp.wrapStream(upStream, streamInfo);
    } catch (e) {
      return cb(e);
    }

    bhttp.request(krakenUrl, {
      method: 'post',
      formFields: {
        data: JSON.stringify(krakenOptions),
        upload: wrappedStream
      }
    }, (err: Error, response: BhttpResponse) => {
      if (err) {
        err.message = `PhotoOptimizier:POST Request:${doc._id}:${err.message}`;
        return cb(err);
      }
      const status = response.body;
      if (status.success) {
        bhttp.get(status.kraked_url, { stream: true }, (getErr: Error, getResponse: NodeJS.ReadableStream) => {
          if (getErr) { return cb(new Error(`PhotoOptimizier:GET Request:${getErr.message}`)); }
          getResponse
            .pipe(downStream)
            .once('finish', () => cb())
            .once('error', (streamErr: Error) =>
              cb(new Error(`PhotoOptimizier:Stream Download:${streamErr.message}`)));
        });
      } else {
        cb(new Error(`PhotoOptimizier:No Success:${status.message}`));
      }
    });
  }
}


function getStreamInfo(doc: PhotoDoc): StreamInfo {
  return {
    filename: getFilename(doc),
    contentType: doc.contentType,
    contentLength: doc.length
  };
}


function getFilename(doc: PhotoDoc): string {
  if (doc.contentType === 'image/jpeg') {
    return doc._id + '.jpg';
  } else if (doc.contentType === 'image/png') {
    return doc._id + '.png';
  } else {
    throw new Error(`PhotoOptimizer: invalid contentType ${doc.contentType}`);
  }
}


interface StreamInfo {
  filename: string;
  contentType: string;
  contentLength: number;
}


interface BhttpResponse extends IncomingMessage {
  body: {
    success: boolean;
    message: string;
    kraked_url: string;
  };
}
