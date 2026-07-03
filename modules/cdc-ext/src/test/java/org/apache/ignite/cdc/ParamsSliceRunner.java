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

package org.apache.ignite.cdc;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.TestClass;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.TestWithParameters;

/**
 * Runs an {@link AbstractReplicationTest} subclass over the parameters matrix slice declared by its
 * {@link ParamsSlice} annotation. Test names are formatted exactly as by {@code Parameterized} with
 * {@code @Parameters(name = "clientType={0}, atomicity={1}, mode={2}, backupCnt={3}")}.
 */
public class ParamsSliceRunner extends Suite {
    /** Test name format, interpreted by {@link MessageFormat}. */
    private static final String PARAMS_FMT = "clientType={0}, atomicity={1}, mode={2}, backupCnt={3}";

    /** */
    public ParamsSliceRunner(Class<?> cls) throws InitializationError {
        super(cls, runners(cls));
    }

    /** @return Runner for each parameters combination of the slice declared on {@code cls}. */
    private static List<Runner> runners(Class<?> cls) throws InitializationError {
        ParamsSlice slice = cls.getAnnotation(ParamsSlice.class);

        if (slice == null)
            throw new InitializationError(cls.getName() + " is not annotated with @" + ParamsSlice.class.getSimpleName());

        TestClass testCls = new TestClass(cls);

        List<Runner> runners = new ArrayList<>();

        for (Object[] params : AbstractReplicationTest.parameters(slice.clientType(), slice.atomicity())) {
            String name = '[' + MessageFormat.format(PARAMS_FMT, params) + ']';

            runners.add(new BlockJUnit4ClassRunnerWithParameters(new TestWithParameters(name, testCls, Arrays.asList(params))));
        }

        return runners;
    }
}
