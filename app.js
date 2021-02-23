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
const { PubSub } = require('@google-cloud/pubsub');
const { BigQuery } = require('@google-cloud/bigquery');
//app.set('view engine', 'pug');

const server = require('http').Server(app);

const WebSocket = require('ws');
const wss = new WebSocket.Server({ server });

wss.on('connection', function connection(ws) {
  var count = 0;
  const pubSubClient = new PubSub();
  const topicName = 'projects/et-eng-cicd-env-build-pipeline/topics/game-session-stream-1';

  ws.on('message', function incoming(message) {
    count++;
    //console.log('received: %s', message);

    const doRespond = count%5==0;

    publishMessage(pubSubClient, message, topicName, ws, doRespond);

  });
});

async function publishMessage(pubSubClient, data, topicName, ws, doRespond) {
  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(data);

  try {
    const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
    if(doRespond){
      const sessionId = JSON.parse(data).sessionId;
      queryScores(ws,sessionId);
    }
    //console.log(`Message ${messageId} published.`);
  } catch (error) {
    ws.send(`Received error while publishing: ${error.message}`);
    //console.error(`Received error while publishing: ${error.message}`);
  }
}

async function queryScores(ws,sessionId) {
  // Queries a public Stack Overflow dataset.

  // Create a client
  const bigqueryClient = new BigQuery();

  // The SQL query to run
  const sqlQuery = `SELECT action, COUNT(*)/SUM(COUNT(*)) OVER () AS ratio FROM \`et-eng-cicd-env-build-pipeline.gameAnalyticsDataset1.game-session-analytics-1\` WHERE sessionId=@sessionId GROUP BY action`;

  const options = {
    query: sqlQuery,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'asia-south1',
    params: {
      sessionId: sessionId
    }
  };

  // Run the query
  const [rows] = await bigqueryClient.query(options);

  let result = {
    avoid: 0,
    jump: 0,
    slide_left: 0,
    slide_right: 0
  };
  let responseData = {
    verticalPos: 'top',
    horizontalPos: 'center'
  };

  if (!rows.length) {
    ws.send(JSON.stringify({ data: responseData }));
  } else {
    rows.map(item => {
      result[item.action] = item.ratio * 100
    });

    if (result.jump < 25) {
      responseData.verticalPos = "top"
    } else {
      responseData.verticalPos = "bottom"
    }

    if (Math.abs(result.slide_left - result.slide_right) <= 5) {
      responseData.horizontalPos = "center"
    } else if (result.slide_right > result.slide_left) {
      responseData.horizontalPos = "left"
    } else if (result.slide_right < result.slide_left) {
      responseData.horizontalPos = "right"
    }

    ws.send(JSON.stringify({ data: responseData }));
  }
}

if (module === require.main) {
  const PORT = process.env.PORT || 8080;
  server.listen(PORT, () => {
    console.log(`App listening on port ${PORT}`);
    console.log('Press Ctrl+C to quit.');
  });
}
// [END appengine_websockets_app]

module.exports = server;
