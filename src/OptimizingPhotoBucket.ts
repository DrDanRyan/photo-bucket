import {Db} from 'mongodb';
import {PhotoBucket} from './PhotoBucket';
import {Readable, Writable} from 'stream';
const bhttp = require('bhttp');
const krakenUrl = 'https://api.kraken.io/v1/upload';

export class OptimizingPhotoBucket extends PhotoBucket {
  constructor(db: Db, private auth: KrakenAuth) {
    super(db);
  }

  optimize(
    upStream: Readable,
    doc: GridFSDoc,
    downStream: Writable,
    cb: ErrorCallback
  ): void {
    const krakenOptions = {
      auth: this.auth,
      wait: true,
      lossy: true
    };

    let streamInfo: StreamInfo;
    try {
      streamInfo = getStreamInfo(doc);
    } catch (e) {
      return cb(e);
    }

    const wrappedStream = bhttp.wrapStream(upStream, streamInfo);

    bhttp.request(krakenUrl, {
      method: 'post',
      formFields: {
        data: JSON.stringify(krakenOptions),
        upload: wrappedStream
      }
    }, (err, response) => {
      if (err) { return cb(new Error(`PhotoOptimizier:POST Request:${err.message}`)); }
      const status = response.body;
      if (status.success) {
        bhttp.get(status.kraked_url, {stream: true}, (getErr, getResponse) => {
          if (getErr) { return cb(new Error(`PhotoOptimizier:GET Request:${getErr.message}`)); }
          getResponse
            .pipe(downStream)
            .once('finish', () => cb())
            .once('error', streamErr =>
              cb(new Error(`PhotoOptimizier:Stream Download:${streamErr.message}`)));
        });
      } else {
        cb(new Error(`PhotoOptimizier:No Success:${status.message}`));
      }
    });
  }
}


function getStreamInfo(doc: GridFSDoc): StreamInfo {
  return {
    filename: getFilename(doc),
    contentType: doc.contentType,
    contentLength: doc.length
  };
}

function getFilename(doc: GridFSDoc): string {
  if (doc.contentType === 'image/jpeg') {
    return doc._id + '.jpg';
  } else if (doc.contentType === 'image/png') {
    return doc._id + '.png';
  } else {
    throw new Error(`PhotoOptimizer: invalid contentType ${doc.contentType}`);
  }
}

export interface GridFSDoc {
  _id: string;
  length: number;
  contentType: string;
}

export interface KrakenAuth {
  api_key: string;
  api_secret: string;
}

export interface ErrorCallback {
  (err?: Error): void;
}

interface StreamInfo {
  filename: string;
  contentType: string;
  contentLength: number;
}
