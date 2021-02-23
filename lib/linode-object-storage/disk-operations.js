"use strict";
/* globals require, module */

/**

  To use google-cloud-storage, you'll need to install its dependency:

  `npm install --save @google-cloud/storage`

  and update config.js with additional parameters:

  CONFIGURED_STORAGE: "google-cloud-storage",
  GOOGLE_CLOUD_STORAGE: {
    BUCKET: "your-bucket-name",
    AUTHENTICATION: {
      projectId: 'your-project-123',
      keyFilename: '/path/to/keyfile.json'
    }
  }

  If you are running on a Google Compute Engine VM, you do not need to
  include AUTHENTICATION (eg. `config.GOOGLE_CLOUD_STORAGE.AUTHENTICATION`
  should return `undefined`).

  You will also need to be sure that the authenticated account has "full"
  permissions to the Storage API:
  https://cloud.google.com/storage/docs/access-control/iam

  Further information at
  https://github.com/GoogleCloudPlatform/google-cloud-node#google-cloud-storage-beta

  JSFS neither endorses nor is endorsed by Google.

**/

const config = require("../../config.js");
const log        = require("../../jlog.js");
// var aws_api    = require("aws-sdk")(config.LINODE_OBJECT_STORAGE.AUTHENTICATION);
const aws_api    = require("aws-sdk");
const bucket = config.LINODE_OBJECT_STORAGE.BUCKET;
const aws_s3_endpoint = new aws_api.Endpoint(config.LINODE_OBJECT_STORAGE.ENDPOINT);
const los = new aws_api.S3({
  endpoint: aws_s3_endpoint,
  accessKeyId: config.LINODE_OBJECT_STORAGE.AUTHENTICATION.ACCESS_KEY_ID,
  secretAccessKey: config.LINODE_OBJECT_STORAGE.AUTHENTICATION.SECRET_ACCESS_KEY
});


// *** CONFIGURATION ***
log.level = config.LOG_LEVEL; // the minimum level of log messages to record: 0 = info, 1 = warn, 2 = error
log.message(log.INFO, "Disk Operation - Linode Object Storage");

los.listBuckets(function(err, data) {
  if (err) {
    log.message(log.DEBUG, "Error " + err);
  } else {
    log.message(log.DEBUG, "Success " + JSON.stringify(data));
  }
});

/**
 * Download file from S3
 * @param {String} attachmentId the attachment id
 * @return {Promise} promise resolved to downloaded data
 */
async function downloadObject (attachmentId) {
 const file = await los.getObject({ Bucket: bucket, Key: attachmentId }).promise()
 return {
  data: file.Body,
  mimetype: file.ContentType
 }
}



// let testCallback = function(result){
//   log.message(log.DEBUG, "got result");
// }



module.exports.read = function(file_path, callback){
  return bucket.file(file_path).download(callback);
};

module.exports.exists = function(file_path, callback){
  return bucket.file(file_path).getMetadata(callback);
};

module.exports.stream_read = function(file_path){
  return bucket.file(file_path).createReadStream();
};

module.exports.write = function(file_path, contents /*[, options], cb */){
  var args = Array.prototype.slice.call(arguments);
  var callback = args.pop();

  return bucket.file(file_path).save(contents, callback);
};

module.exports.delete = function(file_path, callback){
  return bucket.file(file_path).delete(callback);
};
