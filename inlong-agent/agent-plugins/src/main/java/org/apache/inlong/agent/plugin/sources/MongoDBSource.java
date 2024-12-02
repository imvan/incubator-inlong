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
 */

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.extend.DefaultExtendedHandler;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MongoDBSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBSource.class);
    private static final Integer DEBEZIUM_QUEUE_SIZE = 100;
    private ExecutorService executor;
    public InstanceProfile profile;
    private BlockingQueue<SourceData> debeziumQueue;
    private final Properties props = new Properties();
    private String database;
    private String collection;
    private String snapshotMode;

    private boolean isRestoreFromDB = false;

    public MongoDBSource() {
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("MongoDBSource init: {}", profile.toJsonStr());
            debeziumQueue = new LinkedBlockingQueue<>(DEBEZIUM_QUEUE_SIZE);
            database = profile.get(TaskConstants.TASK_MONGO_DATABASE_INCLUDE_LIST);
            collection = profile.get(TaskConstants.TASK_MONGO_COLLECTION_INCLUDE_LIST);
            snapshotMode = profile.get(TaskConstants.TASK_MONGO_SNAPSHOT_MODE, "initial");

            props.setProperty("name", "MongoDB-" + instanceId);
            props.setProperty("connector.class", MongoDbConnector.class.getName());
            props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
            String agentPath = AgentConfiguration.getAgentConf()
                    .get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
            String offsetPath = agentPath + "/" + getThreadName() + "offset.dat";
            props.setProperty("offset.storage.file.filename", offsetPath);

            props.setProperty(String.valueOf(MongoDbConnectorConfig.LOGICAL_NAME), "agent-mongoDB-" + instanceId);
            props.setProperty(String.valueOf(MongoDbConnectorConfig.HOSTS),
                    profile.get(TaskConstants.TASK_MONGO_HOSTS));
            props.setProperty(String.valueOf(MongoDbConnectorConfig.USER), profile.get(TaskConstants.TASK_MONGO_USER));
            props.setProperty(String.valueOf(MongoDbConnectorConfig.PASSWORD),
                    profile.get(TaskConstants.TASK_MONGO_PASSWORD));
            props.setProperty(String.valueOf(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST), database);
            props.setProperty(String.valueOf(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST), collection);
            props.setProperty(String.valueOf(MongoDbConnectorConfig.SNAPSHOT_MODE), snapshotMode);

            executor = Executors.newSingleThreadExecutor();
            executor.execute(startDebeziumEngine());

        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + collection, ex);
        }
    }

    private Runnable startDebeziumEngine() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName() + "debezium");
            try (DebeziumEngine<ChangeEvent<String, String>> debeziumEngine = DebeziumEngine.create(Json.class)
                    .using(props)
                    .using(OffsetCommitPolicy.always())
                    .notifying(this::handleConsumerEvent)
                    .build()) {

                debeziumEngine.run();
            } catch (Throwable e) {
                LOGGER.error("do run error in mongoDB debezium: ", e);
            }
        };
    }

    private void handleConsumerEvent(List<ChangeEvent<String, String>> records,
            RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        boolean offerSuc = false;
        for (ChangeEvent<String, String> record : records) {
            SourceData sourceData = new SourceData(record.value().getBytes(StandardCharsets.UTF_8), "0L");
            while (isRunnable() && !offerSuc) {
                offerSuc = debeziumQueue.offer(sourceData, 1, TimeUnit.SECONDS);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    @Override
    protected String getThreadName() {
        return "mongo-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("mongo collection is {}", collection);
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        try {
            int size = 0;
            while (size < BATCH_READ_LINE_TOTAL_LEN) {
                SourceData sourceData = debeziumQueue.poll(1, TimeUnit.SECONDS);
                if (sourceData != null) {
                    size += sourceData.getData().length;
                    dataList.add(sourceData);
                } else {
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("poll {} data from debezium queue interrupted.", instanceId);
        }
        return dataList;
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        LOGGER.info("release mongo source");
        executor.shutdownNow();
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}
