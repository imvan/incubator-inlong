/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.inlong.sdk.sort.manager;

import java.util.Collection;
import java.util.Set;

import org.apache.inlong.sdk.sort.api.Cleanable;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InLongTopicFetcher;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.entity.InLongTopic;

public abstract class InLongSingleTopicManager implements Cleanable {

    protected ClientContext context;
    protected QueryConsumeConfig queryConsumeConfig;

    public InLongSingleTopicManager(ClientContext context, QueryConsumeConfig queryConsumeConfig) {
        this.context = context;
        this.queryConsumeConfig = queryConsumeConfig;
    }

    public abstract InLongTopicFetcher addFetcher(InLongTopic inLongTopic);

    public abstract InLongTopicFetcher removeFetcher(InLongTopic inLongTopic, boolean closeFetcher);

    public abstract InLongTopicFetcher getFetcher(String fetchKey);

    public abstract Collection<InLongTopicFetcher> getAllFetchers();

    public abstract Set<String> getManagedInLongTopics();

    public abstract void offlineAllTp();

    public abstract void close();

}
