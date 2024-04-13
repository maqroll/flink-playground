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
import static org.apache.flink.table.api.Expressions.call;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
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
public class LateralWithTableAPIJob {

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

    Table countries = tableEnv.from(
        TableDescriptor.forConnector("datagen")
            .option("rows-per-second", "10")
            .option("fields.iso.length","1")
            .schema(
                Schema.newBuilder()
                    .column("iso", DataTypes.STRING())
                    .column("capital", DataTypes.STRING())
                    .build())
            .build());

    TemporalTableFunction ttf = tableEnv.from("input")
        .createTemporalTableFunction($("ts"), $("iso"));

    tableEnv.createTemporarySystemFunction("enrichWithCapital", ttf);

    tableEnv.createTemporaryView("people", peopleStream,
        Schema.newBuilder()
            .columnByExpression("proctime", "PROCTIME()")
            .build());

    tableEnv.from("people")
        .leftOuterJoinLateral(call("enrichWithCapital", $("proctime")))
        //.leftOuterJoinLateral(call("enrichWithCapital", $("proctime")), $("country").isEqual($("iso")))
        .where($("country").isEqual($("iso")))
        .select($("name"), $("country"), $("capital"))
        .execute()
        .print();

    env.execute("Flink Java API Skeleton");
  }
}
