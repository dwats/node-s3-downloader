const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const mkdirp = require('mkdirp');
require('dotenv').config();

const outputDirectory = path.join(__dirname, process.env.OUTPUT_DIRECTORY);
const s3 = new AWS.S3();
const bucket = process.env.S3_BUCKET;

/**
 * Create directory tree from an array of S3 bucket contents and return that array.
 * @param {String} directory File output directory
 * @param {Object} contents S3 bucket contents array.
 * @return {Array} S3 bucket contents
 */
function makeDirectoryTree(directory, contents) {
  contents.forEach((content) => {
    makeDirectory(path.join(directory, content.Key));
  });
  return contents;
}

/**
 * Make file directory path if it doesn't exist.
 * @param {String} filepath 
 */
function makeDirectory(filepath) {
  const dirPath = path.dirname(filepath);
  if (!fs.existsSync(dirPath)) {
    mkdirp(dirPath);
  }
}

/**
 * Return an Object containing S3 Bucket contents
 * @param Bucket {String} S3 bucket to get contents from.
 * @return {Promise<Object>} An object containing S3 bucket contents.
 */
function getS3ObjectList(Bucket) {
  const params = { Bucket };
  return s3.listObjectsV2(params)
    .promise()
    .then((data) => Promise.resolve(data.Contents));
}

/**
 * Return an array of objects containing S3 Bucket content keys and buffers.
 * @param {String} Bucket S3 bucket to get contents from.
 * @param {Object} contents S3 bucket contents array.
 */
function getAllS3ObjectFiles(Bucket, contents) {
  return Promise.all(
    contents
      .filter((content) => content.Key.slice(-1) !== '/')
      .map((content) => getS3Object(Bucket, content.Key))
  );
}

/**
 * Save file from S3 bucket to output directory, preserving bucket file structure.
 * @param {String} Key S3 content object key
 * @return {Promise<Object>} S3 Content object filename (Key) and buffer (Body) 
 */
function getS3Object(Bucket, Key) {
  const params = {
    Bucket,
    Key
  }
  return s3.getObject(params)
    .promise()
    .then((data) => Promise.resolve({
      filename: Key,
      data: data.Body
    }));
}

/**
 * Return promise that all S3 filedata is written to file system.
 * @param {String} directory 
 * @param {Object} filedata S3 Content object filename (Key) and buffer (Body) 
 * @return {Promise}
 */
function writeAllToFile(directory, filedata) {
  return Promise.all(filedata.map((data) => {
    const filepath = path.join(directory, data.filename);
    return writeToFile(data.data, filepath);
  }));
}

/**
 * Return promise that S3 filedata is written to file system.
 * @param {Buffer} buffer S3 content object Body
 * @param {String} filepath 
 * @return {Promise}
 */
function writeToFile(buffer, filepath) {
  return new Promise((resolve, reject) => {
    fs.writeFile(filepath, buffer, (err) => {
      if (err) return reject(err);
      return resolve();
    });
  });
}

getS3ObjectList(bucket)
  .then((s3Contents) => makeDirectoryTree(outputDirectory, s3Contents))
  .then((s3Contents) => getAllS3ObjectFiles(bucket, s3Contents))
  .then((s3FileData) => writeAllToFile(outputDirectory, s3FileData))
  .catch((e) => console.log(e));
