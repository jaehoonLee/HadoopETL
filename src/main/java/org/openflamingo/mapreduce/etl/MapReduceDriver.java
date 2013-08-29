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
package org.openflamingo.mapreduce.etl;

import org.apache.hadoop.util.ProgramDriver;
import org.openflamingo.mapreduce.core.Constants;
import org.openflamingo.mapreduce.etl.groupby.GroupByDriver;

/**
 * 모든 MapReduce를 실행하기 위한 Alias를 제공하는 Program Driver.
 *
 * @author Edward KIM
 * @since 0.1
 */
public class MapReduceDriver {

    public static void main(String argv[]) {
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("groupby", GroupByDriver.class, "지정한 Key를 기준으로 Value를 취합하는 MapReduce ETL");
            programDriver.driver(argv);
            System.exit(Constants.JOB_SUCCESS);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(Constants.JOB_FAIL);
        }
    }
}

