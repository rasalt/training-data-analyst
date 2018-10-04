// Copyright 2017 Google Inc.
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
//
////////////////////////////////////////////////////////////////////////////////
package com.google.cloud.sme.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.sme.common.ActionUtils;
import com.google.cloud.sme.Entities;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ProjectTopicName;
import java.io.*;
/** A basic Pub/Sub publisher for purposes of demonstrating use of the API. */
public class Publisher {
  com.google.cloud.pubsub.v1.Publisher pub;
  /** Creates a new publisher associated with the given project and topic. */
  public Publisher(String project, String topic) {
     try {
        ProjectTopicName topicName = ProjectTopicName.of(project, topic);
        pub = com.google.cloud.pubsub.v1.Publisher.newBuilder(topicName).build();
     } 
     catch(IOException e) {
	       e.printStackTrace();
     }
  }

  /** Publishes the action on the topic associated with this publisher. */
  public void publish(Entities.Action action) {
        ByteString data = ActionUtils.encodeActionAsJson(action);
        String str = data.toStringUtf8();	
        System.out.println("published" + str);
	PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> future = pub.publish(pubsubMessage);
  }
}
