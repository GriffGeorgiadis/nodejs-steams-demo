//Using the Node.js fs module, you can read a file, and serve it over HTTP when a new connection is established to your HTTP server:
const http = require('http')
const fs = require('fs')

const server = http.createServer(function(req, res) {
  fs.readFile(__dirname + '/data.txt', (err, data) => {
    res.end(data)
  })
})
server.listen(3000)




//If the file is big, the operation above will take quite a bit of time. Here is the same thing written using streams:
const http = require('http')
const fs = require('fs')

const server = http.createServer((req, res) => {
  const stream = fs.createReadStream(__dirname + '/data.txt')
  stream.pipe(res)
})
server.listen(3000)
