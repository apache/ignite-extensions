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

package org.apache.ignite.examples.ml.inference.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.MiscUtils;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.dmg.pmml.PMML;
import org.dmg.pmml.regression.RegressionModel;
import org.dmg.pmml.regression.RegressionTable;
import org.jpmml.model.PMMLUtil;

/**
 * Run logistic regression model loaded from PMML file. The PMML file was generated by Spark MLLib toPMML operator.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class LogRegFromSparkThroughPMMLExample {
    /** Run example. */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Logistic regression model loaded from PMML over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start(MiscUtils.resolveIgniteConfig("config/example-ignite.xml"))) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

                String path = MiscUtils.resolveResourceFile("models/spark/iris.pmml")
                    .toPath().toAbsolutePath().toString();
                LogisticRegressionModel mdl = PMMLParser.load(path);

                System.out.println(">>> Logistic regression model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    vectorizer,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));
            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
            }
        }
    }

    /** Util class to build the LogReg model. */
    private static class PMMLParser {
        /**
         * @param path Path.
         */
        public static LogisticRegressionModel load(String path) {
            try (InputStream is = new FileInputStream(new File(path))) {
                PMML pmml = PMMLUtil.unmarshal(is);

                RegressionModel logRegMdl = (RegressionModel)pmml.getModels().get(0);

                RegressionTable regTbl = logRegMdl.getRegressionTables().get(0);

                Vector coefficients = new DenseVector(regTbl.getNumericPredictors().size());

                for (int i = 0; i < regTbl.getNumericPredictors().size(); i++)
                    coefficients.set(i, regTbl.getNumericPredictors().get(i).getCoefficient());

                double interceptor = regTbl.getIntercept();

                return new LogisticRegressionModel(coefficients, interceptor);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}
