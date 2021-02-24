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
 log.message(log.DEBUG, "read: " + file.Body.toString('utf-8'));
 return {
  data: file.Body,
  mimetype: file.ContentType
 }
}

async function getObjectHead (attachmentId) {
 const data = await los.headObject({ Bucket: bucket, Key: attachmentId }).promise()
 log.message(log.DEBUG, attachmentId + " head: " + JSON.stringify(data));
 return {
  data: data,
 }
}

async function putObject (attachmentId, content, contentType) {
 const response = await los.putObject({ Bucket: bucket, Key: attachmentId, Body:content, ContentType:contentType}).promise()
 log.message(log.DEBUG, "put result: " + JSON.stringify(response));
 return {
  data: response,
 }
}

async function deleteObject (attachmentId) {
 const response = await los.deleteObject({ Bucket: bucket, Key: attachmentId}).promise()
 log.message(log.DEBUG, "delete result: " + JSON.stringify(response));
 return {
  data: response,
 }
}


async function isItWorking (pathName, contents) {
  let putResult = await putObject(pathName, contents, "text/plain");
  let headResult = await getObjectHead(pathName);
  let downloadResult = await downloadObject(pathName);
  let deleteResult = await deleteObject(pathName);
  log.message(log.DEBUG, "tested: " + pathName);
}

isItWorking('startup_test.txt', "Testing a little teapot");

// let returnVal = downloadObject('exists.txt');
// returnVal = getObjectHead('exists.txt');
// returnVal = getObjectHead('exists.jpg');
// returnVal = getObjectHead('file_that_doesnt_exist.txt');

// log.message(log.DEBUG, "returned: " + JSON.stringify(returnVal.Body));

// let testCallback = function(result){
//   log.message(log.DEBUG, "got result");
// }

module.exports.read = function(file_path, callback){
  // return bucket.file(file_path).download(callback);
  log.message(log.DEBUG, "starting read for " + file_path);
  return los.getObject({ Bucket: bucket, Key: file_path }, callback).promise();
};

module.exports.exists = async function(file_path, callback){
  // return bucket.file(file_path).getMetadata(callback);
  log.message(log.DEBUG, "starting exists for " + file_path);
  // try {
  //   let result = await los.headObject({ Bucket: bucket, Key: file_path }).promise();
  //   callback(true);
  // } catch (error) {
  //   log.message(log.DEBUG, "doesObjectExist failed with " + error);
  //   callback(false);
  // }
  callback(false);
};

module.exports.stream_read = function(file_path){
  log.message(log.DEBUG, "starting stream_read for " + file_path);
  return bucket.file(file_path).createReadStream();
};

module.exports.write = function(file_path, contents /*[, options], cb */){
  var args = Array.prototype.slice.call(arguments);
  var callback = args.pop();
  log.message(log.DEBUG, "starting write for " + file_path);
  //return bucket.file(file_path).save(contents, callback);
  return los.putObject({ Bucket: bucket, Key: file_path, Body:contents }, callback).promise();
};

module.exports.delete = function(file_path, callback){
  //return bucket.file(file_path).delete(callback);
  log.message(log.DEBUG, "starting delete for " + file_path);
  return los.deleteObject({ Bucket: bucket, Key: file_path }, callback).promise();
};
