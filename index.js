const stream = require('stream');
const util = require('util');
const csv = require('csvtojson');
const { createReadStream, createWriteStream } = require("fs");
const { Transform } = require("stream");


const pipeline = util.promisify(stream.pipeline);
const csvParser = csv();

//Readable Stream
const readStream = createReadStream('./example_data/biostats.csv');

//Transform Stream
const transformStream = new Transform({
  transform(chunk, encoding, cb) {
    try {
      var data = JSON.parse(chunk.toString());

      data.Age = parseInt(data.Age);
      data['Height (in)'] = parseInt(data['Height (in)']);
      data['Weight (lbs)'] = parseInt(data['Weight (lbs)']);

      console.log(data);

      data = Buffer.from(JSON.stringify(data));
      cb(null, data);
    } catch (err) {
      cb(err, null);
    }
  }
});

//writable stream
const writeStream = createWriteStream('./example_data/outputData.txt');

// const writeStream = new Writable({
//   write: (chunk, _, done) => {
//     conn.createQuestions(JSON.parse(chunk.toString()), (err, data) => {
//       if (err) {
//         console.log("Error adding data to qestions DB: ", err);
//       } else {
//         counter++;
//         if (counter % 10000 === 0) {
//           console.log("Added to Questions DB!", counter);
//         }
//         done();
//       }
//     });
//   }
// }),


const streamPipeline = async () => {
  try {
    await pipeline(
      readStream,
      csvParser,
      transformStream,
      writeStream
      );
    console.log("Questions pipeline succeeded");
  } catch (err) {
    console.log("Questions pipeline failed:", err);
  }
};

streamPipeline();
