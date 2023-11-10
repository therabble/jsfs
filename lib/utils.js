"use strict";
/* globals require, module */

var crypto     = require("crypto");
var path       = require("path");
var url        = require("url");
var log        = require("../jlog.js");
var config     = require("../config.js");
// var operations = require("./" + (config.CONFIGURED_STORAGE || "fs") + "/disk-operations.js");
var operations = {};
var operationsStorageByPath = {};
var TOTAL_NON_MOTHBALLED_LOCATIONS = config.STORAGE_LOCATIONS.length;
for(var storage_location in config.STORAGE_LOCATIONS){
  var selected_location = config.STORAGE_LOCATIONS[storage_location];
  if (!operations[selected_location.storage]) { //don't require a storage type twice!
    operations[selected_location.storage] = require("./" + selected_location.storage + "/disk-operations.js");
  }
  operationsStorageByPath[selected_location.path] = selected_location.storage;
  if (selected_location.status === "mothballed") {
    TOTAL_NON_MOTHBALLED_LOCATIONS--;
  }
}
var TOTAL_LOCATIONS = config.STORAGE_LOCATIONS.length;

// simple encrypt-decrypt functions
module.exports.encrypt = function encrypt(data, key){
  var cipher = crypto.createCipher("aes-256-cbc", key);
  cipher.write(data);
  cipher.end();
  return cipher.read();
};

var sha1_to_hex = function sha1_to_hex(data){
  var shasum = crypto.createHash("sha1");
  shasum.update(data);
  return shasum.digest("hex");
};
module.exports.sha1_to_hex = sha1_to_hex;

// save inode to disk
module.exports.save_inode = function save_inode(inode, callback){
  var accessed_locations = 0;
  var paths_for_debug_feedback = "(";

  var _cb = function _cb(error){
    accessed_locations++;
    if(error){
      log.message(log.ERROR, "Error saving inode: " + error);
    } else {
      log.message(log.INFO, "Inode " + accessed_locations + " of " + TOTAL_NON_MOTHBALLED_LOCATIONS + " saved.");
    }
    // log.message(log.DEBUG, "accessed_locations: " + accessed_locations + " TOTAL_NON_MOTHBALLED: " + TOTAL_NON_MOTHBALLED_LOCATIONS);
    if (accessed_locations === TOTAL_NON_MOTHBALLED_LOCATIONS) {
      log.message(log.DEBUG, "inodes all written. " + paths_for_debug_feedback.slice(0,-2) + ") Calling back from _cb.");
      return callback(inode);
    }
  };

  // store a copy of each inode in each storage location for redundancy
  for(var storage_location in config.STORAGE_LOCATIONS){
    var selected_location = config.STORAGE_LOCATIONS[storage_location];
    // operations.write(path.join(selected_location.path, inode.fingerprint + ".json"), JSON.stringify(inode), _cb);
    if (selected_location.status == 'mothballed') {
      // we don't write anything to mothballed storage locations
    } else {
      paths_for_debug_feedback += selected_location.path + ", ";
      operations[selected_location.storage].write(path.join(selected_location.path, inode.fingerprint + ".json"), JSON.stringify(inode), _cb);
    }
  }
};

// load inode from ONLY the first storage in order that contains the requested path's inode
module.exports.load_inode = function load_inode(uri, callback){
  log.message(log.DEBUG, "uri: " + uri);

  // calculate fingerprint
  var inode_fingerprint = sha1_to_hex(uri);

  var _load_inode = function _load_inode(idx){
    var selected_location = config.STORAGE_LOCATIONS[idx]
    var selected_path = selected_location.path;
    var selected_storage = selected_location.storage;
    operations[selected_storage].read(path.join(selected_path, inode_fingerprint + ".json"), function(err, data){
      log.message(log.DEBUG,"idx " + idx + " of " + TOTAL_LOCATIONS + ", operations[" + selected_storage + "]");
      idx++;
      log.message(log.DEBUG, "Reading " + path.join(selected_path, inode_fingerprint + ".json"));
      if (err) {
        if (idx === TOTAL_LOCATIONS) {
          log.message(log.WARN, "Unable to load inode for requested URL: " + uri);
          return callback(err);
        } else {
          log.message(log.DEBUG, "Error loading inode from " + selected_path);
          return _load_inode(idx);
        }
      }

      // var inode = JSON.parse(data);
      // log.message(log.DEBUG, "Inode loaded from " + selected_path + " with '" + data + "'");
      // return callback(null, inode);

      try {
        // log.message(log.INFO, "Inode loading");
        var inode = JSON.parse(data);
        log.message(log.DEBUG, "Inode loaded from " + selected_path + " with '" + data + "'");
        return callback(null, inode);
      } catch(ex) {
        log.message(log.INFO, "In catch block for idx " + (idx-1)); 
        if (idx === TOTAL_LOCATIONS) {
          log.message(log.WARN, "Unable to parse inode for requested URL: " + uri);
          return callback(ex);
        } else {
          log.message(log.DEBUG, "Error parsing inode from " + selected_path);
          return _load_inode(idx);
        }
      }
    });

    //look at     config.STORAGE_LOCATIONS[idx].storage here and do the right type's operation
    // var found=false;
    // var return_error=null;

    // for (; ((idx < TOTAL_LOCATIONS) && !found); idx++) {
    //   log.message(log.DEBUG,"idx " + idx + " of " + TOTAL_LOCATIONS);
    //   log.message(log.DEBUG,"found is " + found);
    //   var selected_path = config.STORAGE_LOCATIONS[idx].path;
    //   log.message(log.DEBUG, "Loading inode " + inode_fingerprint + " from " + selected_path);

    //   const selected_location = config.STORAGE_LOCATIONS[idx];

    //   operations[selected_location.storage].read(path.join(selected_path, inode_fingerprint + ".json"), function(err,data){
    //     log.message(log.DEBUG, "Reading " + path.join(selected_path, inode_fingerprint + ".json"))
    //     if (err) {
    //       log.message(log.DEBUG, "Error loading inode from " + selected_path);
    //       return_error=err;
    //     }
    //     try {
    //       var inode = JSON.parse(data);
    //       log.message(log.INFO, "Inode loaded from " + selected_path);
    //       found=true;
    //       return callback(null, inode);
    //     } catch(ex) {
    //       log.message(log.DEBUG, "Error parsing inode from " + selected_path);
    //       return_error=ex;
    //     }
    //   });
    // }

    // //we didn't find an inode
    // if (!found) {
    //   log.message(log.WARN, "Unable to load inode for requested URL: " + uri);
    //   return callback(return_error);
    // }

    // operations.read(path.join(selected_path, inode_fingerprint + ".json"), function(err, data){
    //   idx++;
    //   if (err) {
    //     if (idx === TOTAL_LOCATIONS) {
    //       log.message(log.WARN, "Unable to load inode for requested URL: " + uri);
    //       return callback(err);
    //     } else {
    //       log.message(log.DEBUG, "Error loading inode from " + selected_path);
    //       return _load_inode(idx);
    //     }
    //   }

    //   try {
    //     var inode = JSON.parse(data);
    //     log.message(log.INFO, "Inode loaded from " + selected_path);
    //     return callback(null, inode);
    //   } catch(ex) {
    //     if (idx === TOTAL_LOCATIONS) {
    //       log.message(log.WARN, "Unable to parse inode for requested URL: " + uri);
    //       return callback(ex);
    //     } else {
    //       log.message(log.DEBUG, "Error parsing inode from " + selected_path);
    //       return _load_inode(idx);
    //     }
    //   }
    // });
  };

  _load_inode(0);
};

module.exports.commit_block_to_disk = function commit_block_to_disk(block, block_object, balanced_storage_target_indexes, callback) {
  // if storage locations exist, save the block to disk
  var total_locations = config.STORAGE_LOCATIONS.length;
  // var min_nonmirror_locations = config.MINIMUM_BALANCED_LOCATIONS_FOR_A_BLOCK;

  if(total_locations > 0){

    // check all non-mothballed storage locations to see if we already have this block

    // var on_complete = function on_complete(found_block){
    //   // TODO: consider increasing found count to enable block redundancy
    //   if(!found_block){

    //     // write new block to next storage location
    //     // TODO: consider implementing in-band compression here
    //     var dir = config.STORAGE_LOCATIONS[next_storage_location].path;
    //     // only safe for little text test files: log.message(log.DEBUG, 'commit_block_to_disk: writing block ' + block + ',' + JSON.stringify(block_object) + ' to storage location ' + next_storage_location + ' at dir ' + dir)
    //     log.message(log.DEBUG, 'commit_block_to_disk: writing block' + JSON.stringify(block_object) + ' to storage location ' + next_storage_location + ' at dir ' + dir)
    //     operations.write(dir + block_object.block_hash, block, "binary", function(err){
    //       if (err) {
    //         return callback(err);
    //       }

    //       block_object.last_seen = dir;
    //       log.message(log.INFO, "New block " + block_object.block_hash + " written to " + dir);

    //       return callback(null, block_object);

    //     });

    //   } else {
    //     log.message(log.INFO, "Duplicate block " + block_object.block_hash + " not written to disk");
    //     return callback(null, block_object);
    //   }
    // };

    var write_needed_blocks = function write_needed_blocks(target_array, writes_to_go, success_counter){
      // TODO: consider increasing found count to enable block redundancy  GALA update: instead of a found count, we have an array of write targets.
      log.message(log.DEBUG,"target_array_length " + target_array.length + "  writes_to_go: " + writes_to_go + " success_counter: " + success_counter);
      if (target_array.length > 0) {
        // TODO: consider implementing in-band compression here
        // for (let w = 0; w < target_array.length; w++) {
        //   var dir = config.STORAGE_LOCATIONS[target_array[w]].path;
        //   // only safe for little text test files: log.message(log.DEBUG, 'commit_block_to_disk: writing block ' + block + ',' + JSON.stringify(block_object) + ' to storage location ' + next_storage_location + ' at dir ' + dir)
        //   log.message(log.DEBUG, 'commit_block_to_disk: writing block' + JSON.stringify(block_object) + ' to storage location ' + target_array[w] + ' at dir ' + dir)
        //   operations[config.STORAGE_LOCATIONS[target_array[w]].storage].write(dir + block_object.block_hash, block, "binary", function(err){
        //     if (err) {
        //       return callback(err);
        //     }

        //     block_object.last_seen = dir;
        //     log.message(log.INFO, "New block " + block_object.block_hash + " written to " + dir);

        //     return callback(null, block_object);

        //   });
        // }
        var dir = config.STORAGE_LOCATIONS[target_array[0]].path;
        var selected_storage = config.STORAGE_LOCATIONS[target_array[0]].storage;
        var whichAmI = target_array[0];

        operations[selected_storage].write(dir + block_object.block_hash, block, "binary", function(err){
          log.message(log.DEBUG,"writing first of " + target_array.length + ", operations[" + selected_storage + "]");
          writes_to_go--;

          if (err) { 
            log.message(log.WARN, "Unable to write block " + block_object.block_hash + " to storage index: " + whichAmI);
            if (writes_to_go) {
              target_array.splice(0,1); //remove the first item in list.
              return write_needed_blocks(target_array, writes_to_go, success_counter);
            } else {
              if (success_counter > 0) {
                // at least one write succeeded, and there are no writes_to_go, so report success, of sorts. 
                if (config.STORAGE_INDEX_FOR_NEW_LAST_SEEN_PATHS > -1) {
                  block_object.last_seen = config.STORAGE_LOCATIONS[config.STORAGE_INDEX_FOR_NEW_LAST_SEEN_PATHS].path;
                } else {
                  block_object.last_seen = dir;
                }
                return callback(null, block_object);  
              } else {
                log.message(log.WARN, "No writes appear to have succeeded, returning final err from write_needed_blocks");
                return callback(err);
              }
            }
          } else {
            success_counter++;
            log.message(log.INFO, "New block " + block_object.block_hash + " written to " + dir);
            if (writes_to_go) {
              target_array.splice(0,1); //remove the first item in list.
              return write_needed_blocks(target_array, writes_to_go, success_counter);
            } else {
              if (config.STORAGE_INDEX_FOR_NEW_LAST_SEEN_PATHS > -1) {
                block_object.last_seen = config.STORAGE_LOCATIONS[config.STORAGE_INDEX_FOR_NEW_LAST_SEEN_PATHS].path;
              } else {
                block_object.last_seen = dir;
              }
              return callback(null, block_object);
            }
          }

          try {
            // log.message(log.INFO, "Inode loading");
            var inode = JSON.parse(data);
            log.message(log.DEBUG, "Inode loaded from " + selected_path + " with '" + data + "'");
            return callback(null, inode);
          } catch(ex) {
            log.message(log.INFO, "In catch block for idx " + idx-1); 
            if (idx === TOTAL_LOCATIONS) {
              log.message(log.WARN, "Unable to parse inode for requested URL: " + uri);
              return callback(ex);
            } else {
              log.message(log.DEBUG, "Error parsing inode from " + selected_path);
              return _load_inode(idx);
            }
          }
        });    
      } else {
        // log.message(log.INFO, "Duplicate block " + block_object.block_hash + " not written to disk");
        log.message(log.INFO, "Duplicate blocks for " + block_object.block_hash + " seem to be in the places it needs to be, or there are insufficient valid places.");
        return callback(null, block_object);
      }
    };


    var locate_blocks = function locate_blocks(idx, needed_write_targets){
      var location = config.STORAGE_LOCATIONS[idx];
      var file = location.path + block_object.block_hash;
      var storageType = location.storage;
      var status = location.status;
      var thisIdx = idx;
      idx++;

      if (status != 'mothballed') {
        operations[storageType].exists(file + ".gz", function(err, result){

          if (result) {
            log.message(log.INFO, "Duplicate compressed block " + block_object.block_hash + " found in " + location.path);
            block_object.last_seen = location.path;
            if (status === "balanced" || status === "mirror") {
              // if this is balanced, remove this or another balanced from the list of needed write targets, since it's already present.
              // if this is a mirror, remove this.
              if (needed_write_targets && (needed_write_targets.indexOf(thisIdx) > -1)) {
                //removing ourselves
                needed_write_targets.splice(needed_write_targets.indexOf(thisIdx), 1);
              } else if (status === "balanced") {
                // removing another balanced one, if present, so we don't write more than required by MINIMUM_BALANCED_LOCATIONS_FOR_A_BLOCK
                for (let k = 0; k < needed_write_targets; k++) {
                  if (config.STORAGE_LOCATIONS[k].status === "balanced") {
                    needed_write_targets.splice(k, 1);
                    break;
                  }
                }
              }
            }
            if (idx >= total_locations) {
              return write_needed_blocks(needed_write_targets, needed_write_targets.length, 0);
            } else {
              locate_blocks(idx, needed_write_targets);
            }
          } else {
            operations[storageType].exists(file, function(err_2, result_2){

              if (err_2) {
                log.message(log.INFO, "Block " + block_object.block_hash + " not found in " + location.path);
              }

              if (result_2) {
                log.message(log.INFO, "Duplicate block " + block_object.block_hash + " found in " + location.path);
                block_object.last_seen = location.path;
                if (status === "balanced" || status === "mirror") {
                  // if this is balanced, remove this or another balanced from the list of needed write targets, since it's already present.
                  // if this is a mirror, remove this.
                  if (needed_write_targets && (needed_write_targets.indexOf(thisIdx) > -1)) {
                    //removing ourselves
                    needed_write_targets.splice(needed_write_targets.indexOf(thisIdx), 1);
                  } else if (status === "balanced") {
                    // removing another balanced one, if present, so we don't write more than required by MINIMUM_BALANCED_LOCATIONS_FOR_A_BLOCK
                    for (let k = 0; k < needed_write_targets; k++) {
                      if (config.STORAGE_LOCATIONS[k].status === "balanced") {
                        needed_write_targets.splice(k, 1);
                        break;
                      }
                    }
                  }
                }
              }

              if (idx >= total_locations) {
                return write_needed_blocks(needed_write_targets, needed_write_targets.length, 0);
              } else {
                locate_blocks(idx, needed_write_targets);
              }
            });
          }
        });
      } else {
        // it is mothballed, so it's automatically not found there and won't be written there. keep going.
        if (idx >= total_locations) {
          return write_needed_blocks(needed_write_targets, needed_write_targets.length, 0);
        } else {
          locate_blocks(idx, needed_write_targets);
        }
      }
    };

    for (let i = 0; i < config.STORAGE_LOCATIONS.length; i++) {
      // add in all mirror locations.
      if (config.STORAGE_LOCATIONS[i].status === "mirror") {
        balanced_storage_target_indexes.push(i);
      }
    }

    locate_blocks(0, balanced_storage_target_indexes);

  } else {
    log.message(log.WARN, "No storage locations configured, block not written to disk");
    return callback(null, block_object);
  }
};

// Use analyze data to identify offset until non-zero audio, grab just that portion to store.

// In analyze we identified the "data" starting byte and block_align ((Bit Size * Channels) / 8)
// We'll start the scan at block.readUInt32LE([data chunk offset] + 8) in order to find the
// start of non-zero audio data, and slice off everything before that point as a seperate block.
// That way we can deduplicate tracks with slightly different silent leads.
module.exports.wave_audio_offset = function wave_audio_offset(block, data, default_size){
  // block_align most likely to be 4, but it'd be nice to handle alternate cases.
  // Essentially, we should use block["readUInt" + (block_align * 8) + "LE"]() to scan the block.
  var block_align = data.data_block_size;

  if (data.subchunk_id === "data" && block_align === 4) {
     // start of audio subchunk + 4 bytes for the label ("data") + 4 bytes for size)
    var data_offset  = data.subchunk_byte + 4 + 4;
    var block_length = block.length;

    // Increment our offset by block_align, since we're analyzing on the basis of it.
    for (data_offset; (data_offset + block_align) < block_length; data_offset = data_offset + block_align) {
      if (block.readUInt32LE(data_offset) !== 0) {
        log.message(log.INFO, "Storing the first " + data_offset + " bytes seperately");
        // return the offset with first non-zero audio data;
        return data_offset;
      }
    }
    // if we didn't return out of the for loop, return default
    return default_size;

  } else {
    // If we didn't find a data chunk, return default
    return default_size;
  }
};

function dasherize(s){
  return s.replace(/_/g, '-');
}

function to_header(param, obj){
  var prefix = obj[param];
  return (prefix ? prefix + "-" : "") + dasherize(param);
}

module.exports.request_parameters = function request_parameters(accepted_params, uri, headers){
  var q = url.parse(uri, true).query;

  return accepted_params.reduce(function(o,p){
    var _p = Object.keys(p)[0];
    o[_p] = q[_p] || headers[to_header(_p, p)];
    return o;
  }, {});
};

module.exports.target_from_url = function target_from_url(hostname, uri) {
  var parsed = url.parse(uri);
  var pathname = parsed.pathname;
  hostname = hostname.split(":")[0];

  if (pathname.substring(0,2) !== "/.") {
    return "/" + hostname.split(".").reverse().join(".") + pathname;
  } else {
    return "/" + pathname.substring(2);
  }
};
