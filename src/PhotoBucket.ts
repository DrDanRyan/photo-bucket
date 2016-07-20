import {GridFSBucket, Db} from 'mongodb';
import {Readable, Writable, Transform} from 'stream';
import {createWriteStream, createReadStream} from 'fs';
import {id as generateId} from 'meteor-random';
import {basename} from 'path';
import {auto} from 'async';
const crypto = require('crypto');
const sharp = require('sharp');

export class PhotoBucket extends GridFSBucket {
  private photoTransforms: PhotoTransformDict = {};


  constructor(db: Db, name = 'photos') {
    super(db, {bucketName: name});
  }


  findOne(query: string|Object, cb: ResultCallback): void;
  findOne(query: string|Object, fields: Object, cb: ResultCallback): void;
  findOne(query: string|Object, arg1?: any, arg2?: any): void {
    if (typeof query === 'string') {
      query = {_id: query};
    }

    if (arg2 === undefined) {
      this.find(query).limit(1).next(arg1);
    } else {
      this.find(query).limit(1).project(arg1).next(arg2);
    }
  }


  download(_id: string, dest: string|Writable, cb: ResultCallback): void {
    const downloadStream = this.openDownloadStream(_id);

    let destStream: Writable;
    if (typeof dest === 'string') {
      destStream = createWriteStream(dest);
    } else {
      destStream = dest;
    }

    destStream.once('finish', cb).once('error', cb);
    downloadStream.pipe(destStream);
  }


  upload(fullPath: string, uploadCb: ResultCallback): void {
    auto({
      metadata: (cb: ResultCallback) => this.metadata(fullPath, cb),
      checksum: (cb: ResultCallback) => this.checksum(fullPath, cb),
      doc: ['metadata', 'checksum', doInsert]
    }, (err, res) => uploadCb(err, res.doc));

    function doInsert(res: any, cb: ResultCallback): void {
      const metadata: SharpMetadata = res.metadata;
      const checksum: string = res.checksum;
      const filename = basename(fullPath);
      const doc = {
        _id: generateId(),
        filename,
        contentType: `image/${metadata.format}`,
        metadata: {
          type: 'original',
          space: metadata.space,
          height: metadata.height,
          width: metadata.width,
          checksum
        }
      };

      const readStream = createReadStream(fullPath);
      const uploadStream = this.openUploadStreamWithId(doc._id, filename, {
        contentType: doc.contentType,
        metadata: doc.metadata
      }).once('finish', () => cb(null, doc)).once('error', cb);
      readStream.pipe(uploadStream);
    }
  }


  metadata(src: string|Readable, cb: ResultCallback): void {
    sharp(src).metadata((err: Error, metadata: SharpMetadata) => {
      if (err) { return cb(err); }
      if (!metadata) { return cb(new Error(`Metadata Error: metadata for ${src} is empty`)); }
      cb(null, metadata);
    });
  }


  checksum(src: string|Readable, cb: ResultCallback): void {
    let srcStream: Readable;
    if (typeof src === 'string') {
      srcStream = createReadStream(src);
    } else {
      srcStream = src;
    }

    const hash: Readable & Writable = crypto.createHash('sha1')
      .once('readable', () => {
        const checksum = hash.read();
        if (!checksum) { return cb(new Error('Checksum Empty')); }
        cb(null, checksum.toString('hex'));
      })
      .once('error', cb);
    srcStream.pipe(hash);
  }


  transform(_id: string, type: string, cb: ResultCallback): void {
    if (this.photoTransforms[type] === undefined) {
      process.nextTick(() => cb(new Error(`No transform for type ${type} has been registered.`)));
    }

    this.findOne(_id, (err: Error, doc: GridFSDoc): void => {
      if (err) { return cb(err); }
      if (!doc) { return cb(new Error('Optimize Error: _id not found')); }
      const newId = generateId();
      const newMetadata: PhotoMetadata = {
        type,
        isOptimized: false,
        checksum: doc.metadata.checksum,
        space: doc.metadata.space,
        width: doc.metadata.width,
        height: doc.metadata.height
      };

      let newContentType: string;
      let fileTransform: PhotoTransform;
      if (this.shouldMakeJpeg(doc)) {
        newContentType = 'image/jpeg';
        fileTransform = this.photoTransforms[type](doc).quality(100).jpeg();
      } else {
        newContentType = doc.contentType;
        fileTransform = this.photoTransforms[type](doc);
      }

      const downloadStream = this.openDownloadStream(_id);
      const uploadStream = this.openUploadStreamWithId(newId, doc.filename, {
        contentType: newContentType,
        metadata: newMetadata
      }).once('finish', () => cb(null, newId)).once('error', cb);
      downloadStream.pipe(fileTransform).pipe(uploadStream);
    });
  }


  registerType(type: string, transform: (doc: GridFSDoc) => PhotoTransform): void {
    this.photoTransforms[type] = transform;
  }


  private shouldMakeJpeg(doc: GridFSDoc): boolean {
    return (doc.contentType !== 'image/jpeg' && doc.contentType !== 'image/png') ||
      !/rgb/.test(doc.metadata.space);
  }
}


export interface ResultCallback {
  (err?: Error, result?: any): void;
}


export interface ErrorCallback {
  (err?: Error): void;
}


export interface GridFSDoc {
  _id: string;
  length: number;
  contentType: string;
  filename: string;
  metadata: PhotoMetadata;
}


interface SharpMetadata {
  space: string;
  height: number;
  width: number;
  format: string;
}


interface PhotoMetadata {
  type: string;
  checksum: string;
  isOptimized: boolean;
  height: number;
  width: number;
  space: string;
}


interface PhotoTransform extends Transform {
  quality(val: number): PhotoTransform;
  jpeg(): PhotoTransform;
}


interface PhotoTransformDict {
  [index: string]: (doc: GridFSDoc) => PhotoTransform;
}
