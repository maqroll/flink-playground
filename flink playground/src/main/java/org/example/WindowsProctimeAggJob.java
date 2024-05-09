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

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WindowsProctimeAggJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setParallelism(1);

    tableEnv.executeSql(
        "CREATE TABLE input ("
            + " `iso` STRING,"
            + " `capital` STRING,"
            // Use it at inner join
            // + " `ts` TIMESTAMP_LTZ(3)"
            // Use it at outer join
            + " `ts` AS PROCTIME()"
            + ") WITH ("
            + " 'connector' = 'datagen',"
            + " 'fields.iso.length' = '1',"
            + " 'fields.capital.length' = '1',"
            + " 'rows-per-second' = '1'"
            + ")");

    tableEnv.executeSql("""
            CREATE TABLE OUTPUT (
             `name` STRING,
             `country` STRING,
             `capital` STRING
            ) WITH (
             'connector' = 'print'
            )""");


    Table inner = tableEnv.sqlQuery("""
        SELECT
         iso,
         capital,
         window_start,
         count(*)
         FROM (
          SELECT
            iso,
            capital,
            ts,
            window_start,
            window_end,
            window_time
          FROM TABLE(TUMBLE(TABLE input, DESCRIPTOR(ts), INTERVAL '1' MINUTES))
        ) GROUP BY iso, capital, window_start, window_end, window_time
        """);


    tableEnv.toChangelogStream(inner).print("all");

    // It's bidirectional so both sides trigger updates!!

    //System.out.println(env.getExecutionPlan());
    env.execute("Flink Java API Skeleton");
  }
}
