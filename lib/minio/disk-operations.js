const Minio = require("minio");
const config = require("../../config.js");
const log = require("../../jlog.js");

// TODO: Move this to a minio config block?
const bucket = config.LINODE_OBJECT_STORAGE.BUCKET;

// Initialize client
const minioClient = new Minio.Client({
  endPoint: config.LINODE_OBJECT_STORAGE.ENDPOINT,
  port: 443,
  useSSL: true,
  accessKey: config.LINODE_OBJECT_STORAGE.AUTHENTICATION.ACCESS_KEY_ID,
  secretKey: config.LINODE_OBJECT_STORAGE.AUTHENTICATION.SECRET_ACCESS_KEY,
})

// Implement read
module.exports.read = function readFile(filePath, callback){

  log.message(log.DEBUG, `Got read()`);
  
  var data;
  var chunks = [];
  var readStream = minioClient.getObject(bucket, filePath);

  // DEBUG
  console.log(readStream);
  
  function onData(chunk){
    log.message(log.DEBUG, `Got a chunk ${len(chunk)} bytes long`);
    chunks.push(chunk);
  }
  function onEnd() {    
    data = Buffer.concat(chunks);
    log.message(log.DEBUG, `Got end event, returning ${len(data)} bytes`);
    return callback(undefined, data);
  }
  function onError(err){
    log.message(log.ERROR, `Error reading data ${err}`);
    console.log(err);
    return callback(err, data);
  }

  readStream.on("data", onData);
  readStream.on("end", onEnd);
  readStream.on("error", onError);
}

// TODO: Implement stream_read (maybe all reads should be streaming?)
module.exports.stream_read = function streamRead(filePath){
  
}

// Implement write
module.exports.write = function writeFile(...args){
  // TODO: See if we can fix this weird variable args handling
  const [filePath, contents, ...restArgs] = args;
  let [contentType, callback] = restArgs;

  if (typeof contentType === 'function' && !callback) {
    callback = contentType;
    contentType = undefined;
  }
  
  if (!contentType && typeof contents === 'string') {
    contentType = 'text/plain';
  }

  minioClient.putObject(bucket, filePath, contents, function (err, etag) {
    if (err) {
      log.message(log.ERROR, `Error writing object: ${err}`);
      console.log(err);
      return callback(err, undefined);
    }

    log.message(log.DEBUG, `Write complete, etag: ${etag}`);
    return callback(err, etag);
  });
}

// TODO: Implement delete
module.exports.delete = function deleteFile(filePath, callback){
  
}

// TODO: Implement exists
module.exports.exists = function exists(filePath, callback){
  
}

// Self-test
function selfTest(pathName, contents){
  log.message(log.DEBUG, `Starting self test using ${pathName}`);
  module.exports.write(pathName, contents, function(wErr, wResult){
    if(wErr){
      return log.message(log.ERROR, `WRITE error during self test ${wErr}`);
    }
    module.exports.read(pathName, function(rErr, rResult){
      log.message(log.DEBUG, `Calling read()`);
      if(rErr){
        return log.message(log.ERROR, `READ error during self test ${rErr}`);
      }
      // TODO: Test delete
    });
    log.message(log.DEBUG, "self test complete!");
  });
}

selfTest("minio_self_test.txt", "I am a teapot");
