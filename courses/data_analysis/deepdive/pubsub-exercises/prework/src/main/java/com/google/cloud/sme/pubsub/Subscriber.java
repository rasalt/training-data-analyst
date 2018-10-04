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

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.sme.common.ActionUtils;
import com.google.cloud.sme.Entities;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.protobuf.ByteString;

/** A basic Pub/Sub subscriber for purposes of demonstrating use of the API. */
public class Subscriber {
  long viewcount=0;
  com.google.cloud.pubsub.v1.Subscriber sub;
  /** Creates a new subscriber associated with the given project and subscription. */
  public Subscriber(String project, String subscription) {
    MessageReceiver receiver =
    new MessageReceiver() {
     @Override
     public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		   consumer.ack();
		   if (isViewAction(ActionUtils.decodeActionFromJson(message.getData()))) {
			   ByteString data = message.getData(); 
			   String str = data.toStringUtf8();
			   System.out.println("Id : " + message.getMessageId());
			   System.out.println("Data : " + str);
 			viewcount++;
		   }
     }
  };
  /** Creates a new subscriber associated with the given project and subscription. */



     ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
     sub = com.google.cloud.pubsub.v1.Subscriber.newBuilder(subscriptionName, receiver).build();
     sub.startAsync();
  }

  /** Returns the number of VIEW action seen */
  public long getViewCount() {
   	  
    return viewcount;
  }

  /** Returns true if action is a VIEW action. */
  private boolean isViewAction(Entities.Action action) {
    return action.getAction() == Entities.Action.ActionType.VIEW;
  }
}
