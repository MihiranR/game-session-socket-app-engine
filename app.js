// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// [START appengine_websockets_app]
const app = require('express')();
app.set('view engine', 'pug');

const server = require('http').Server(app);

const WebSocket = require('ws');
const wss = new WebSocket.Server({server});

wss.on('connection', function connection(ws) {
  var count = 0;
  ws.on('message', function incoming(message) {
    count++;
    console.log('received: %s', message);
    if(count%5==0){
      ws.send(count.toString());
    }
  });
});

if (module === require.main) {
  const PORT = process.env.PORT || 8080;
  server.listen(PORT, () => {
    console.log(`App listening on port ${PORT}`);
    console.log('Press Ctrl+C to quit.');
  });
}
// [END appengine_websockets_app]

module.exports = server;
