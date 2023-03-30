"use strict";
/* globals require, module */
// *** CONFIGURATION ***

const config = require('../../config.js');
const log = require('../../jlog.js');
var fs      = require("fs");

log.level = config.LOG_LEVEL; // the minimum level of log messages to record: 0 = info, 1 = warn, 2 = error
log.message(log.INFO, 'Disk Operation - fs - File System');


module.exports.exists      = fs.stat;
module.exports.read        = fs.readFile;
module.exports.stream_read = fs.createReadStream;
module.exports.write       = fs.writeFile;
module.exports.delete      = fs.unlink;
