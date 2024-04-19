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

package org.apache.inlong.sort.formats.inlongmsgtlogcsv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;
import org.apache.inlong.sort.formats.util.FormatUtils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InLongMsgTlogCsvFormatFactory}.
 */
public class InLongMsgTlogCsvFormatFactoryTest {

    public RowFormatInfo testFormatInfo;

    public TypeInformation<RowData> testTypeInformation;

    public ResolvedSchema resolvedSchema;

    public DataType dataType;

    @Before
    public void setup() {
        resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("time", DataTypes.TIMESTAMP()),
                        Column.physical("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                        Column.physical("student_name", DataTypes.STRING()),
                        Column.physical("score", DataTypes.INT()),
                        Column.physical("date", DataTypes.DATE()));
        dataType = resolvedSchema.toPhysicalRowDataType();
        RowType rowType = (RowType) dataType.getLogicalType();
        testTypeInformation = InternalTypeInfo.of(rowType);
        testFormatInfo = new RowFormatInfo(
                new String[]{"time", "attributes", "student_name", "score", "date"},
                new FormatInfo[]{
                        new TimestampFormatInfo(),
                        new MapFormatInfo(StringFormatInfo.INSTANCE, StringFormatInfo.INSTANCE),
                        StringFormatInfo.INSTANCE,
                        IntFormatInfo.INSTANCE,
                        new DateFormatInfo("yyyy-MM-dd")
                });
    }

    @Test
    public void testDeSchema() {
        final InLongMsgTlogCsvDeserializationSchema expectedDeSer =
                new InLongMsgTlogCsvDeserializationSchema.Builder(
                        testFormatInfo)
                                .setCharset("UTF-8")
                                .setDelimiter(';')
                                .setQuoteCharacter('\'')
                                .setEscapeCharacter('\\')
                                .setNullLiteral("n/a")
                                .build();
        final Map<String, String> options = getAllOptions();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeSer, actualDeser);
    }

    private DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(resolvedSchema, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        return sourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, dataType);
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", InLongMsgTlogCsvFormatFactory.IDENTIFIER);
        options.put("inlong-msg-tlogcsv.row.format.info", FormatUtils.marshall(testFormatInfo));
        options.put("inlong-msg-tlogcsv.format.field-delimiter", ";");
        options.put("inlong-msg-tlogcsv.format.quote-character", "'");
        options.put("inlong-msg-tlogcsv.format.escape-character", "\\");
        options.put("inlong-msg-tlogcsv.format.null-literal", "n/a");
        return options;
    }
}
