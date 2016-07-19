import {GridFSBucket, Db} from 'mongodb';

export class PhotoBucket extends GridFSBucket {
  constructor(db: Db, name = 'photos') {
    super(db, {bucketName: name});
  }


}
