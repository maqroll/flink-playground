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

package org.example;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class CrossJoinJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setMaxParallelism(240);
    env.setParallelism(10);

    tableEnv.getConfig().getConfiguration()
        .set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, -1) // set the default parallelism explicitly
        .setLong("table.exec.state.ttl", 10000000);

    DataStream<Row> countriesStream = env.fromCollection(
        List.of(
            Row.of("a",
                List.of(Row.of("b"))),
            Row.of("b", List.of())),
        Types.ROW_NAMED(
            new String[]{"id", "items"},
            Types.STRING,
            Types.LIST(
                Types.ROW_NAMED(
                    new String[]{"item_id"},
                    Types.STRING)))
    );

    tableEnv.createTemporaryView("input",
        countriesStream,
        Schema.newBuilder()
            .build());

    String query = """
        SELECT
          input.id AS id, 
          T.item_id AS item_id 
        FROM input
        LEFT OUTER JOIN UNNEST(input.items) AS T(item_id) ON TRUE""";

    Table outerJoin = tableEnv.sqlQuery(query);

    tableEnv.toChangelogStream(outerJoin).print("cross join");

    System.out.println(env.getExecutionPlan());
    env.execute("Flink Java API Skeleton");
  }
}
