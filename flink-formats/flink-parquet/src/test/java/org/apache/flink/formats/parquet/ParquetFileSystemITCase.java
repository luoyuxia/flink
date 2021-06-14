/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet;

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

/** ITCase for {@link ParquetFileFormatFactory}. */
@RunWith(Parameterized.class)
public class ParquetFileSystemITCase extends BatchFileSystemITCaseBase {

    private final boolean configure;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public ParquetFileSystemITCase(boolean configure) {
        this.configure = configure;
    }

    @Override
    public String[] formatProperties() {
        List<String> ret = new ArrayList<>();
        ret.add("'format'='parquet'");
        if (configure) {
            ret.add("'parquet.utc-timezone'='true'");
            ret.add("'parquet.compression'='gzip'");
        }
        return ret.toArray(new String[0]);
    }

    @Override
    public void testNonPartition() {
        super.testNonPartition();

        // test configure success
        File directory = new File(URI.create(resultPath()).getPath());
        File[] files =
                directory.listFiles((dir, name) -> !name.startsWith(".") && !name.startsWith("_"));
        Assert.assertNotNull(files);
        Path path = new Path(URI.create(files[0].getAbsolutePath()));

        try {
            ParquetMetadata footer =
                    readFooter(new Configuration(), path, range(0, Long.MAX_VALUE));
            if (configure) {
                Assert.assertEquals(
                        "GZIP",
                        footer.getBlocks().get(0).getColumns().get(0).getCodec().toString());
            } else {
                Assert.assertEquals(
                        "UNCOMPRESSED",
                        footer.getBlocks().get(0).getColumns().get(0).getCodec().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void before() {
        super.before();
        super.tableEnv()
                .executeSql(
                        String.format(
                                "create table parquetFilterTable ("
                                        + "x string,"
                                        + "y int,"
                                        + "a tinyint,"
                                        + "b smallint,"
                                        + "c int,"
                                        + "d bigint,"
                                        + "e float,"
                                        + "f double,"
                                        + "g string,"
                                        + "h boolean,"
                                        + "i decimal(8,4),"
                                        + "j date,"
                                        + "k timestamp"
                                        + ") with ("
                                        + "'connector' = 'filesystem',"
                                        + "'path' = '%s',"
                                        + "%s)",
                                super.resultPath(), String.join(",\n", formatProperties())));
    }

    @Test
    public void testParquetFilterPushDown() throws ExecutionException, InterruptedException {
        super.tableEnv()
                .executeSql(
                        "insert into parquetFilterTable select x, y, cast(y as tinyint), "
                                + "cast(y as smallint), y, b, "
                                + "cast(y * 3.14 as float) as e, "
                                + "cast(y * 3.1415 as double) as f, "
                                + "case when a = 1 then null else x end as g, "
                                + "case when y >= 10 then false else true end as h, "
                                + "y * 3.14 as i, "
                                + "date '2020-01-01' as j, "
                                + "timestamp '2020-01-01 05:20:00' as k "
                                + "from originalT")
                .await();

        check(
                "select x, y from parquetFilterTable where x = 'x11' and 11 = y",
                Collections.singletonList(Row.of("x11", "11")));

        check(
                "select x, y from parquetFilterTable where 4 <= y and y < 8 and x <> 'x6'",
                Arrays.asList(Row.of("x4", "4"), Row.of("x5", "5"), Row.of("x7", "7")));

        check(
                "select x, y from parquetFilterTable where x = 'x1' and not y >= 3",
                Collections.singletonList(Row.of("x1", "1")));

        check(
                "select x, y from parquetFilterTable where h and y > 2 and y < 4",
                Collections.singletonList(Row.of("x3", "3")));

        check(
                "select x, y from parquetFilterTable where g is null and x = 'x5'",
                Collections.singletonList(Row.of("x5", "5")));

        check(
                "select x, y from parquetFilterTable where g is not null and y > 25",
                Arrays.asList(Row.of("x26", "26"), Row.of("x27", "27")));

        check(
                "select x, y from parquetFilterTable where (g is not null and y > 26) or (g is null and x = 'x3')",
                Arrays.asList(Row.of("x3", "3"), Row.of("x27", "27")));
    }
}
