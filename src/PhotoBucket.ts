import { GridFSBucket, Db } from 'mongodb';
import { createWriteStream, createReadStream } from 'fs';
import { basename } from 'path';
import { parallel } from 'async';
import { createHash } from 'crypto';
const Random = require('meteor-random');
const sharp = require('sharp');
export { Db };

export class PhotoBucket extends GridFSBucket {
  protected photoTransforms: PhotoTransformDict = {};


  constructor(db: Db, name = 'photos') {
    super(db, { bucketName: name });
  }


  findOne(query: string | Object, cb: ResultCb<PhotoDoc>): void;
  findOne(query: string | Object, options: { fields: Object }, cb: ResultCb<PhotoDoc>): void;
  findOne(query: string | Object, arg1?: any, arg2?: any): void {
    if (typeof query === 'string') {
      query = { _id: query };
    }

    if (arg2 === undefined) {
      this.find(query).limit(1).next(arg1);
    } else {
      this.find(query).limit(1).project(arg1.fields).next(arg2);
    }
  }


  download(_id: string, dest: string | NodeJS.WritableStream, cb: ErrorCb): void {
    const downloadStream = this.openDownloadStream(_id as any);

    let destStream: NodeJS.WritableStream;
    if (typeof dest === 'string') {
      destStream = createWriteStream(dest);
    } else {
      destStream = dest;
    }

    destStream.once('finish', cb).once('error', cb);
    downloadStream.pipe(destStream);
  }


  upload(fullPath: string, uploadCb: ResultCb<PhotoDoc>): void;
  upload(fullPath: string, filename: string, uploadCb: ResultCb<PhotoDoc>): void;
  upload(fullPath: string, arg1: any, arg2?: any): void {
    const filename: string = arg2 ? arg1 : basename(fullPath);
    const uploadCb: ResultCb<PhotoDoc> = arg2 ? arg2 : arg1;

    parallel({
      metadata: (cb: any) => this.metadata(fullPath, cb),
      checksum: (cb: any) => this.checksum(fullPath, cb),
    }, (err: Error, res: { metadata: SharpMetadata, checksum: string }) => {
      if (err) { return uploadCb(err); }

      const {metadata, checksum} = res;
      if (['png', 'jpeg', 'tiff', 'webp'].indexOf(metadata.format) === -1) {
        return uploadCb(new Error(`metdata: format of ${metadata.format} is not supported`));
      }

      const doc = {
        _id: Random.id(),
        filename,
        contentType: `image/${metadata.format}`,
        metadata: {
          type: 'original',
          space: metadata.space,
          height: metadata.height,
          width: metadata.width,
          checksum
        }
      } as PhotoDoc;

      const readStream = createReadStream(fullPath);
      const uploadStream = this.openUploadStreamWithId(doc._id, filename, {
        contentType: doc.contentType,
        metadata: doc.metadata
      }).once('finish', () => uploadCb(null, doc)).once('error', uploadCb);
      readStream.pipe(uploadStream);
    });
  }


  metadata(src: string | NodeJS.ReadableStream, cb: ResultCb<SharpMetadata>): void {
    sharp(src).metadata((err: Error, metadata: SharpMetadata) => {
      if (err) { return cb(err); }
      if (!metadata) { return cb(new Error(`metadata: metadata for ${src} is empty`)); }
      cb(null, metadata);
    });
  }


  checksum(src: string | NodeJS.ReadableStream, cb: ResultCb<string>): void {
    let srcStream: NodeJS.ReadableStream;
    if (typeof src === 'string') {
      srcStream = createReadStream(src);
    } else {
      srcStream = src;
    }

    const hash: NodeJS.ReadWriteStream = createHash('sha1')
      .once('readable', () => {
        const checksum: any = hash.read();
        if (!checksum) { return cb(new Error('Checksum Empty')); }
        cb(null, checksum.toString('hex'));
      })
      .once('error', cb);
    srcStream.pipe(hash);
  }


  transform(_id: string, type: string, cb: ResultCb<PhotoDoc>): void {
    if (this.photoTransforms[type] === undefined) {
      process.nextTick(() => cb(new Error(`No transform for type ${type} has been registered.`)));
    }

    this.findOne(_id, (err: Error, doc: PhotoDoc): void => {
      if (err) { return cb(err); }
      if (!doc) { return cb(new Error('Transform Error: _id not found')); }
      const newId = Random.id();
      const metadata: PhotoMetadata = {
        type,
        isOptimized: false,
        checksum: doc.metadata.checksum,
        space: doc.metadata.space,
        width: doc.metadata.width,
        height: doc.metadata.height
      };
      const {transform, contentType} = this.photoTransforms[type](doc);

      const newDoc = {
        _id: newId,
        filename: doc.filename,
        contentType: contentType || doc.contentType,
        metadata
      } as PhotoDoc;

      const downloadStream = this.openDownloadStream(_id as any);
      const uploadStream = this.openUploadStreamWithId(newId, doc.filename, {
        contentType,
        metadata
      }).once('finish', () => cb(null, newDoc)).once('error', cb);
      downloadStream.pipe(transform).pipe(uploadStream);
    });
  }


  registerTransform(name: string, transform: PhotoTransform): void {
    this.photoTransforms[name] = transform;
  }
}


export interface ResultCb<T> {
  (err: Error, result?: T): void;
}


export interface ErrorCb {
  (err?: Error): void;
}


export interface PhotoDoc {
  _id: string;
  length: number;
  contentType: string;
  filename: string;
  metadata: PhotoMetadata;
}


export interface SharpMetadata {
  space: string;
  height: number;
  width: number;
  format: string;
}


export interface PhotoMetadata {
  type: string;
  checksum: string;
  isOptimized: boolean;
  height: number;
  width: number;
  space: string;
}



export interface PhotoTransform  {
  (doc?: PhotoDoc): {
    transform: NodeJS.ReadWriteStream;
    contentType?: string;
  };
}


export interface PhotoTransformDict {
  [name: string]: PhotoTransform;
}
