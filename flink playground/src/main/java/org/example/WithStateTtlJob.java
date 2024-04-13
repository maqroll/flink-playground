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

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WithStateTtlJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setParallelism(1);

    tableEnv.getConfig().getConfiguration()
        .setLong("table.exec.state.ttl", 10000000);

    DataStream<Row> peopleStream = env.fromCollection(
        List.of(Row.of("Alice", 12, "a"),
            Row.of("Bob", 5, "b"),
            Row.of("Peter", 13, "c"),
            Row.of("Paul", 13, "d")),
        Types.ROW_NAMED(
            new String[]{"name", "age", "country"},
            Types.STRING,
            Types.INT,
            Types.STRING)
    );

    tableEnv.executeSql(
        "CREATE TABLE input ("
            + " `iso` STRING,"
            + " `capital` STRING,"
            // Use it at inner join
            + " `ts` TIMESTAMP_LTZ(3)"
            // Use it at outer join
            // + " `ts` AS PROCTIME()"
            + ") WITH ("
            + " 'connector' = 'datagen',"
            + " 'fields.iso.length' = '1',"
            + " 'rows-per-second' = '4'"
            + ")");

    tableEnv.executeSql("""
            CREATE TABLE OUTPUT (
             `name` STRING,
             `country` STRING,
             `capital` STRING
            ) WITH (
             'connector' = 'print'
            )""");


    tableEnv.createTemporaryView("people", peopleStream,
        Schema.newBuilder()
            .columnByExpression("proctime", "PROCTIME()")
            .build());

    String query = """
        SELECT /*+ STATE_TTL('p'='1s', 'e'='90d') */
        p.name, 
        p.country, 
        e.capital 
        FROM people p 
        LEFT OUTER JOIN input e
        ON p.country = e.iso""";

    Table inner = tableEnv.sqlQuery(query);

    Table all = tableEnv.sqlQuery("""
             SELECT iso, capital,ts
             FROM input""");

    tableEnv.toChangelogStream(inner).print("inner join");
    tableEnv.toChangelogStream(all).print("all");

    // It's bidirectional so both sides trigger updates!!

    System.out.println(env.getExecutionPlan());
    env.execute("Flink Java API Skeleton");
  }
}
