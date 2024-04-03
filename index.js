// Loads and processes Readv2 and CTV3 dictionaries
// Assumes that the CTV3 dictionary is available as a single file dict.txt and Readv2 is
// codes.dict.txt and drugs.dict.txt. This was available from TRUD at the time of writing.

// IT IS UNLIKELY WE'LL NEED TO RUN THIS AGAIN, BUT RETAINING IN CASE BUG FIXES REQUIRED.

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, dirname, sep, posix } from 'path';
import { fileURLToPath } from 'url';
import { compress } from 'brotli';
import 'dotenv/config';
const __dirname = dirname(fileURLToPath(import.meta.url));

const DIR = join(__dirname, 'files', 'raw');
const CTV3_DIR = join(DIR, 'CTV3', '20180401');
const READv2_DIR = join(DIR, 'Readv2', 'v20160401');
const ctv3Dir = 'CTV3-20180401';
const readv2Dir = 'Readv2-20160401';
const OUT_CTV3_DIR = join(__dirname, 'files', 'processed', ctv3Dir);
const OUT_READv2_DIR = join(__dirname, 'files', 'processed', readv2Dir);

const defsCTV3 = 'defs-ctv3.json';
const defsCTV3Readable = 'defs-ctv3-readable.json';
const relationsCTV3 = 'relations-ctv3.json';
const relsCTV3Readable = 'relations-ctv3-readable.json';
const defsReadv2 = 'defs-readv2.json';
const defsRv2Readable = 'defs-readv2-readable.json';
const relationsReadv2 = 'relations-readv2.json';
const relationsReadv2Readable = 'relations-readv2-readable.json';

function processHierarchyFile(file, defs = {}, rels = {}) {
  readFileSync(file, 'utf8')
    .split('\n')
    .map((row) => {
      const [code, description, parent] = row.trim().split('\t');
      if (!defs[code]) defs[code] = [description];
      else defs[code].push(description);
      if (!rels[parent]) rels[parent] = {};
      rels[parent][code] = true;
    });
  return { defs, rels };
}
function processCTV3() {
  console.log('> Loading and processing CTV3 files...');
  const { defs, rels } = processHierarchyFile(join(CTV3_DIR, 'dict.txt'));
  writeFileSync(
    join(OUT_CTV3_DIR, defsCTV3Readable),
    JSON.stringify(defs, null, 2)
  );
  writeFileSync(join(OUT_CTV3_DIR, defsCTV3), JSON.stringify(defs));
  writeFileSync(
    join(OUT_CTV3_DIR, relsCTV3Readable),
    JSON.stringify(rels, null, 2)
  );
  writeFileSync(join(OUT_CTV3_DIR, relationsCTV3), JSON.stringify(rels));
  return defs;
}
function processReadv2() {
  console.log('> Loading and processing Readv2 non-drug files...');
  const { defs, rels } = processHierarchyFile(
    join(READv2_DIR, 'codes.dict.txt')
  );
  console.log('> Loading and processing Readv2 drug files...');
  const result = processHierarchyFile(
    join(READv2_DIR, 'drugs.dict.txt'),
    defs,
    rels
  );
  writeFileSync(
    join(OUT_READv2_DIR, defsRv2Readable),
    JSON.stringify(result.defs, null, 2)
  );
  writeFileSync(join(OUT_READv2_DIR, defsReadv2), JSON.stringify(result.defs));
  writeFileSync(
    join(OUT_READv2_DIR, relationsReadv2Readable),
    JSON.stringify(result.rels, null, 2)
  );
  writeFileSync(
    join(OUT_READv2_DIR, relationsReadv2),
    JSON.stringify(result.rels)
  );
  return defs;
}

async function brotliCompress(files) {
  function brot(file) {
    const brotFile = `${file}.br`;
    if (existsSync(brotFile)) {
      console.log(
        `> The file ${brotFile} already exists so no need to compress.`
      );
      return;
    }
    console.log(`> Compressing ${file}...`);
    const result = compress(readFileSync(file), {
      extension: 'br',
      quality: 11, //compression level - 11 is max
    });
    console.log(`> Compressed. Writing to ${brotFile}...`);
    writeFileSync(brotFile, result);
  }

  for (let { file, path } of files) {
    brot(join(path, file));
  }
}

import {
  S3Client,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { fail } from 'assert';

let s3;
async function uploadToS3(file, brotliFile) {
  const posixFilePath = file.split(sep).join(posix.sep);
  const params = {
    Bucket: 'nhs-drug-refset',
    Key: posixFilePath,
  };

  const exists = await s3
    .send(new HeadObjectCommand(params))
    .then((x) => {
      console.log(`> ${file} already exists in R2 so skipping...`);
      return true;
    })
    .catch((err) => {
      if (err.name === 'NotFound') return false;
    });

  if (!exists) {
    console.log(`> ${file} does not exist in R2. Uploading...`);
    await s3.send(
      new PutObjectCommand({
        Bucket: 'nhs-drug-refset',
        Key: posixFilePath,
        Body: readFileSync(brotliFile),
        ContentEncoding: 'br',
        ContentType: 'application/json',
      })
    );
    console.log('> Uploaded.');
  }
}

async function uploadToR2(files) {
  const accessKeyId = `${process.env.ACCESS_KEY_ID}`;
  const secretAccessKey = `${process.env.SECRET_ACCESS_KEY}`;
  const endpoint = `https://${process.env.ACCOUNT_ID}.r2.cloudflarestorage.com`;

  s3 = new S3Client({
    region: 'auto',
    credentials: {
      accessKeyId,
      secretAccessKey,
    },
    endpoint,
  });

  for (let { file, folder } of files) {
    const jsonFile = join('files', 'processed', folder, file);
    const brFile = join('files', 'processed', folder, `${file}.br`);
    await uploadToS3(jsonFile, brFile);
  }
}

function generateTrie(defs, path) {
  console.log('> Generating word list for trie...');
  const words = {};
  Object.values(defs).forEach((definitionArray) => {
    definitionArray
      .join(' ')
      .toLowerCase()
      .replace(/[^a-zA-Z0-9]/g, ' ')
      .split(' ')
      .forEach((word) => {
        if (word.length > 0) words[word] = true;
      });
  });
  console.log(`Extracted ${Object.keys(words).length} words.`);
  const trie = {};
  console.log('> Generating trie...');
  Object.keys(words)
    .sort()
    .forEach((word) => {
      let pointer = trie;
      const lastLetter = word.slice(-1);
      const stub = word.slice(0, -1);
      stub.split('').forEach((letter, i) => {
        if (pointer[letter] && typeof pointer[letter] === 'object') {
          // already there and already an object
          if (i === stub.length - 1) {
            pointer[letter][lastLetter] = true;
          }
        } else if (pointer[letter] === true) {
          // already there but a boolean
          pointer[letter] = { 1: true };
          if (i === stub.length - 1) {
            pointer[letter][lastLetter] = true;
          }
        } else if (pointer[letter]) {
          // already there but a string
          const val = pointer[letter];
          pointer[letter] = {};
          pointer[letter][val] = true;
          if (i === stub.length - 1) {
            pointer[letter][lastLetter] = true;
          }
        } else {
          if (i === stub.length - 1) pointer[letter] = lastLetter;
          else pointer[letter] = {};
        }
        pointer = pointer[letter];
      });
    });
  console.log('> Writing trie.json...');
  writeFileSync(join(path, `trie.json`), JSON.stringify(trie));
  return trie;
}

function processWords(defs, path) {
  const words = {};
  Object.keys(defs).forEach((key, i) => {
    defs[key]
      .join(' ')
      .replace(/[^a-zA-Z0-9]/g, ' ')
      .split(' ')
      .forEach((word) => {
        if (
          word.length >= 2 &&
          [
            'of',
            'with',
            'uk',
            'for',
            'and',
            'to',
            'in',
            'ml',
            'or',
            'by',
            'on',
            'at',
            'vi',
          ].indexOf(word) === -1
        ) {
          if (!words[word.toLowerCase()]) {
            words[word.toLowerCase()] = [key];
          } else {
            words[word.toLowerCase()].push(key);
          }
        }
      });
  });
  console.log('> Writing words.json...');
  writeFileSync(join(path, `words.json`), JSON.stringify(words));
}

const ctv3Defs = processCTV3();
generateTrie(ctv3Defs, OUT_CTV3_DIR);
processWords(ctv3Defs, OUT_CTV3_DIR);
const readv2Defs = processReadv2();
generateTrie(readv2Defs, OUT_READv2_DIR);
processWords(readv2Defs, OUT_READv2_DIR);

const files = [
  { file: defsCTV3, folder: ctv3Dir, path: OUT_CTV3_DIR },
  { file: defsReadv2, folder: readv2Dir, path: OUT_READv2_DIR },
  { file: relationsCTV3, folder: ctv3Dir, path: OUT_CTV3_DIR },
  { file: relationsReadv2, folder: readv2Dir, path: OUT_READv2_DIR },
  { file: 'trie.json', folder: ctv3Dir, path: OUT_CTV3_DIR },
  { file: 'trie.json', folder: readv2Dir, path: OUT_READv2_DIR },
  { file: 'words.json', folder: ctv3Dir, path: OUT_CTV3_DIR },
  { file: 'words.json', folder: readv2Dir, path: OUT_READv2_DIR },
];
brotliCompress(files);
uploadToR2(files);
