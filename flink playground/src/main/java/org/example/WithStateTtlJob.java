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

public class WithStateTtlJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setMaxParallelism(240);
    env.setParallelism(10);

    tableEnv.getConfig().getConfiguration()
        .set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, -1) // set the default parallelism explicitly
        .setLong("table.exec.state.ttl", 10000000);

    tableEnv.executeSql(
        "CREATE TABLE people ("
            + " `name` STRING,"
            + " `age` INT,"
            // Use it at inner join
            + " `country` STRING"
            // Use it at outer join
            // + " `ts` AS PROCTIME()"
            + ") WITH ("
            + " 'connector' = 'datagen',"
            + " 'fields.country.length' = '1',"
            + " 'fields.name.length' = '2',"
            + " 'rows-per-second' = '1'"
            + ")");

    DataStream<Row> countriesStream = env.fromCollection(
        List.of(Row.of("a", "a"),
            Row.of("a", "b"),
            Row.of("a", "c"),
            Row.of("a", "d")),
        Types.ROW_NAMED(
            new String[]{"iso", "capital"},
            Types.STRING,
            Types.STRING)
    );

    tableEnv.createTemporaryView("input", countriesStream,
        Schema.newBuilder()
            // Without this restriction the join will produce four rows for 'a'
            .primaryKey("iso")
            .build());

    String query = """
        SELECT /*+ STATE_TTL('people'='1ms', 'input'='90d') */
          people.name AS name, 
          people.country AS country, 
          input.capital AS capital 
        FROM people
        LEFT OUTER JOIN input
        ON people.country = input.iso""";

    Table outerJoin = tableEnv.sqlQuery(query);

    tableEnv.toChangelogStream(outerJoin).print("outer join");

    // It's bidirectional so both sides trigger updates
    // but setting people's ttl to a minimun in practice avoids it
    // and keep state size to a minimum.

    System.out.println(env.getExecutionPlan());
    env.execute("Flink Java API Skeleton");
  }
}
