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
package org.apache.ignite.gatling.action.cache

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.session.Expression
import io.gatling.core.session.Session

/**
 * Helper to resolve arguments represented as a sequence of expressions.
 */
trait ArgumentsResolveSupport {
    /**
     * Resolves sequence of arguments using the session context.
     *
     * @param session Session.
     * @param arguments List of arguments to resolve.
     * @return List of the resolved arguments.
     */
    def resolveArguments(session: Session, arguments: Seq[Expression[Any]]): Validation[List[Any]] =
        arguments
            .foldLeft(List[Any]().success) { (acc, v) =>
                acc.flatMap(acc => v(session).map(v => v :: acc))
            }
            .map(l => l.reverse)
}
