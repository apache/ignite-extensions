/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.ml.inference.catboost;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MiscUtils;
import org.apache.ignite.ml.catboost.CatboostRegressionModelParser;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.builder.AsyncModelBuilder;
import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * This example demonstrates how to import Catboost model and use imported model for distributed inference in Apache
 * Ignite.
 */
public class CatboostRegressionModelParserExample {
    /**
     * Test model resource name.
     * */
    private static final String TEST_MODEL_RES = "models/catboost/model_reg.cbm";

    /**
     * Test data.
     */
    private static final String TEST_DATA_RES = "datasets/boston_housing_dataset.txt";

    /**
     * Test expected results.
     */
    private static final String TEST_ER_RES = "datasets/boston_housing_dataset-catboost-expected-results.txt";

    /**
     * Parser.
     */
    private static final CatboostRegressionModelParser parser = new CatboostRegressionModelParser();

    /**
     * Run example.
     */
    public static void main(String... args) throws ExecutionException, InterruptedException,
        FileNotFoundException {
        try (Ignite ignite = Ignition.start(MiscUtils.resolveIgniteConfig("config/example-ignite.xml"))) {
            File mdlRsrc = MiscUtils.resolveResourceFile(TEST_MODEL_RES);
            if (mdlRsrc == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_MODEL_RES + "]");

            ModelReader reader = new FileSystemModelReader(mdlRsrc.getPath());
            AsyncModelBuilder mdlBuilder = new IgniteDistributedModelBuilder(ignite, 4, 4);

            File testData = MiscUtils.resolveResourceFile(TEST_DATA_RES);
            if (testData == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_DATA_RES + "]");

            File testExpRes = MiscUtils.resolveResourceFile(TEST_ER_RES);
            if (testExpRes == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_ER_RES + "]");

            try (Model<NamedVector, Future<Double>> mdl = mdlBuilder.build(reader, parser);
                 Scanner testDataScanner = new Scanner(testData);
                 Scanner testExpResultsScanner = new Scanner(testExpRes)) {
                String[] columns = new String[] {
                    "f_0",
                    "f_1",
                    "f_2",
                    "f_3",
                    "f_4",
                    "f_5",
                    "f_6",
                    "f_7",
                    "f_8",
                    "f_9",
                    "f_10",
                    "f_11",
                    "f_12",
                };

                while (testDataScanner.hasNextLine()) {
                    String testDataStr = testDataScanner.nextLine();
                    String testExpResultsStr = testExpResultsScanner.nextLine();

                    HashMap<String, Double> testObj = new HashMap<>();
                    String[] values = testDataStr.split(",");

                    for (int i = 0; i < columns.length; i++) {
                        testObj.put(columns[i], Double.valueOf(values[i]));
                    }

                    double prediction = mdl.predict(VectorUtils.of(testObj)).get();
                    double expPrediction = Double.parseDouble(testExpResultsStr);

                    System.out.println("Expected: " + expPrediction + ", prediction: " + prediction);
                }
            }
        }
        finally {
            System.out.flush();
        }
    }
}
