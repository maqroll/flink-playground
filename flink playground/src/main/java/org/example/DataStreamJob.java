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

import static org.apache.flink.table.api.Expressions.$;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setParallelism(1);

    // According to [doc](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/joins/#processing-time-temporal-join)
    // the way to go to enrich a stream with a dimension table (last/current value)
    // is to use a TemporalTableFunction.

    // Besides is noted that "Currently only inner join and left outer join with temporal tables are supported."

    // So left outer join is supported.

    DataStream<Row> peopleStream = env.fromCollection(
        List.of(Row.of("Alice", 12, "ES"),
            Row.of("Bob", 5, "ES"),
            Row.of("Peter", 13, "CA"),
            Row.of("Paul", 13, "IT")),
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
            + " 'number-of-rows' = '4'"
            + ")");

    TemporalTableFunction ttf = tableEnv.from("input")
        .createTemporalTableFunction($("ts"), $("iso"));

    tableEnv.createTemporarySystemFunction("enrichWithCapital", ttf);

    tableEnv.createTemporaryView("people", peopleStream,
        Schema.newBuilder()
            .columnByExpression("proctime", "PROCTIME()")
            .build());

     Table inner = tableEnv.sqlQuery("SELECT "
         + "p.name, "
         + "p.country, "
         + "e.capital "
         + "FROM people p,"
         + "LATERAL TABLE (enrichWithCapital(p.proctime)) AS e "
         + "WHERE p.country = e.iso");

    // org.apache.flink.table.api.ValidationException:
    // Left outer joins with a table function do not accept a predicate such as `p`.`country` = `e`.`iso`. Only literal TRUE is accepted.
    // tableEnv.sqlQuery("SELECT "
    //     + "* "
    //     + "FROM people p "
    //     + "LEFT OUTER JOIN LATERAL TABLE (enrichWithCapital(p.proctime)) AS e "
    //     + "ON p.country = e.iso");

    // org.apache.flink.table.api.ValidationException:
    // Only single column join key is supported. Found [I@50e1f3fc in [Temporal Table Function]
     Table outer = tableEnv.sqlQuery("SELECT "
         + "* "
         + "FROM people p "
         + "LEFT JOIN LATERAL TABLE (enrichWithCapital(p.proctime)) AS e "
         + "ON TRUE");

     //tableEnv.toDataStream(inner).print("inner join");
    tableEnv.toDataStream(outer).print("outer join");

    env.execute("Flink Java API Skeleton");
  }
}
