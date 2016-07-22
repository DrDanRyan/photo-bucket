import {Db} from 'mongodb';
import {PhotoBucket, PhotoDoc, ErrorCb, ResultCb} from './PhotoBucket';
import {Readable, Writable} from 'stream';
import {IncomingMessage} from 'http';
import {id as generateId} from 'meteor-random';
const bhttp = require('bhttp');
const krakenUrl = 'https://api.kraken.io/v1/upload';

export {Db};

export class OptimizingPhotoBucket extends PhotoBucket {
  constructor(db: Db, private auth: {api_secret: string, api_key: string}) {
    super(db);
  }

  optimize(doc: PhotoDoc, cb: ResultCb<string>): void {
    const newId = generateId();
    const newMetadata = {
      type: doc.metadata.type,
      isOptimized: false,
      checksum: doc.metadata.checksum,
      space: doc.metadata.space,
      height: doc.metadata.height,
      width: doc.metadata.width
    };

    const readStream = this.openDownloadStream(doc._id);
    const writeStream = this.openUploadStreamWithId(newId, doc.filename, {
      contentType: doc.contentType,
      metadata: newMetadata
    });
    this.sendToKraken(readStream, doc, writeStream, (err: Error) => cb(err, newId));
  }

  private sendToKraken(upStream: Readable, doc: PhotoDoc, downStream: Writable, cb: ErrorCb): void {
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
      if (err) { return cb(new Error(`PhotoOptimizier:POST Request:${err.message}`)); }
      const status = response.body;
      if (status.success) {
        bhttp.get(status.kraked_url, {stream: true}, (getErr: Error, getResponse: Readable) => {
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
