/**
 * Module dependencies
 */
const debug = require("debug")("skipper-gcs");
const qs = require("querystring");
const Writable = require("stream").Writable;
const _ = require("lodash");
const gcloud = require("gcloud");
const concat = require("concat-stream");
const mime = require("mime");
const gm = require("gm");

/**
 * skipper-gcs
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */
module.exports = function GCSStore(globalOpts) {
  globalOpts = globalOpts || {};
  _.defaults(globalOpts, {
    email: "",
    bucket: "",
    folder: "photos",
    scopes: ["https://www.googleapis.com/auth/devstorage.full_control"],
  });
  const gcs = gcloud.storage({
    projectId: globalOpts.projectId,
    keyFilename: globalOpts.keyFilename,
  });
  const bucket = gcs.bucket(globalOpts.bucket);

  const adapter = {
    ls(dirname, cb) {
      bucket.getFiles(
        {
          prefix: dirname,
        },
        (err, files) => {
          if (err) {
            cb(err);
          } else {
            files = _.pluck(files, "name");
            cb(null, files);
          }
        }
      );
    },
    read(fd, cb) {
      const remoteReadStream = bucket.file(fd).createReadStream();
      remoteReadStream
        .on("error", err => {
          cb(err);
        })
        .on("response", response => {
          // Server connected and responded with the specified status and headers.
        })
        .on("end", () => {
          // The file is fully downloaded.
        })
        .pipe(
          concat(data => {
            cb(null, data);
          })
        );
    },
    rm(fd, cb) {
      return cb(new Error("TODO"));
    },
    /**
     * A simple receiver for Skipper that writes Upstreams to Google Cloud Storage
     *
     * @param  {Object} options
     * @return {Stream.Writable}
     */
    receive: function GCSReceiver(options) {
      options = options || {};
      options = _.defaults(options, globalOpts);
      const receiver__ = Writable({
        objectMode: true,
      });
      // This `_write` method is invoked each time a new file is received
      // from the Readable stream (Upstream) which is pumping filestreams
      // into this receiver.  (filename === `__newFile.filename`).
      receiver__._write = function onFile(__newFile, encoding, done) {
        const metadata = {};
        _.defaults(metadata, options.metadata);
        metadata.contentType = mime.lookup(__newFile.fd);
        const allUploads = [];
        allUploads.push(
          new Promise((resolve, reject) => {
            const file = bucket.file(`${globalOpts.folder}/${__newFile.fd}`);
            const stream = file.createWriteStream({
              metadata,
            });
            stream.on("error", err => {
              reject(err);
            });
            stream.on("finish", () => {
              __newFile.extra = file.metadata;
              __newFile.extra.Location = `https://storage.googleapis.com/${globalOpts.bucket}/${globalOpts.folder}/${__newFile.fd}`;
              if (globalOpts.public) file.makePublic();
              resolve();
            });
            __newFile.pipe(stream);
          })
        );
        if (globalOpts.resize) {
          allUploads.push(
            new Promise((resolve, reject) => {
              const file = bucket.file(`photos/resized/${__newFile.fd}`);
              const stream = file.createWriteStream({
                metadata,
              });
              gm(__newFile)
                .resize(`${globalOpts.resizeDimension || 900}>`)
                .gravity("Center")
                .stream()
                .pipe(stream);
              stream.on("error", err => {
                reject(err);
              });
              stream.on("finish", () => {
                if (globalOpts.public) file.makePublic();
                resolve();
              });
            })
          );

          // Add thumb
          allUploads.push(
            new Promise((resolve, reject) => {
              const file = bucket.file(`photos/thumbs/${__newFile.fd}`);
              const stream = file.createWriteStream({
                metadata,
              });
              gm(__newFile)
                .resize(100, 100, "^")
                .gravity("Center")
                .crop("100", "100")
                .stream()
                .pipe(stream);
              stream.on("error", err => {
                reject(err);
              });
              stream.on("finish", () => {
                if (globalOpts.public) file.makePublic();
                resolve();
              });
            })
          );
        }
        Promise.all(allUploads)
          .then(() => done())
          .catch(err => receiver__.emit("error", err));
      };

      return receiver__;
    },
  };
  return adapter;
};
