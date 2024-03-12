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
package org.apache.ignite.gatling.builder.transaction

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef.exec
import io.gatling.core.Predef.group
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.IgniteDslInvalidConfigurationException
import org.apache.ignite.gatling.action.ignite.TransactionCloseAction
import org.apache.ignite.gatling.action.ignite.TransactionCommitAction
import org.apache.ignite.gatling.action.ignite.TransactionRollbackAction
import org.apache.ignite.gatling.action.ignite.TransactionStartAction
import org.apache.ignite.gatling.builder.cache.CacheActionCommonParameters
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * DSL to create transaction operations.
 */
trait Transactions {
    /**
     * Start constructing of the transaction.
     *
     * @return TransactionBuilderParametersStep.
     */
    def tx: TransactionBuilderParametersStep = TransactionBuilderParametersStep(TransactionParameters())

    /**
     * Implicit to build transaction chain with the default request name.
     *
     * @param transactionBuilder: TransactionBuilder.
     * @return Protocol.
     */
    implicit def transactionBuilder2ChainBuilder(transactionBuilder: TransactionBuilder): ChainBuilder =
        transactionBuilder.build()

    /**
     * Create transaction commit request with the default request name.
     *
     * @return TransactionCommitActionBuilder.
     */
    def commit: TransactionCommitActionBuilder = TransactionCommitActionBuilder()

    /**
     * Create transaction commit request with the default request name.
     *
     * @return TransactionRollbackActionBuilder.
     */
    def rollback: TransactionRollbackActionBuilder = TransactionRollbackActionBuilder()
}

/**
 * Transaction parameters.
 *
 * @param concurrency Concurrency.
 * @param isolation Isolation.
 * @param timeout Timeout in milliseconds.
 * @param size Number of entries participating in transaction (may be approximate).
 */
case class TransactionParameters(
    concurrency: Option[TransactionConcurrency] = None,
    isolation: Option[TransactionIsolation] = None,
    timeout: Option[Long] = None,
    size: Option[Int] = None
) {
    /**
     * Checks correctness of the parameters combination.
     *
     * @return true if parameters are valid.
     */
    def areValid: Boolean =
        noneDefined(concurrency, isolation, timeout, size) ||
            (allDefined(concurrency, isolation) && noneDefined(timeout, size)) ||
            (allDefined(concurrency, isolation, timeout) && size.isEmpty) ||
            allDefined(concurrency, isolation, timeout, size)

    private def allDefined(options: Option[Any]*): Boolean =
        options.forall(element => element.isDefined)

    private def noneDefined(options: Option[Any]*): Boolean =
        options.forall(element => element.isEmpty)

    override def toString: String =
        s"[concurrency=$concurrency, isolation=$isolation, timeout=$timeout, size=$size]"
}

/**
 * Transaction builder.
 *
 * @param params Transaction parameters collected so far.
 * @param transactionChain Chain of user actions to be executed within a transaction.
 */
case class TransactionBuilder(params: TransactionParameters, transactionChain: Seq[ChainBuilder]) extends NameGen {

    /**
     * Builds full chain of actions that make up a transaction.
     *
     * Full chain consists of:
     *  - transaction start action
     *  - chain of user-provided actions
     *  - close transaction action.
     *
     * @param requestName Request name.
     * @return Full chain of actions.
     */
    def as(requestName: String): ChainBuilder =
        group(_ => requestName.success)(
            exec(TransactionStartActionBuilder(requestName, params))
                .exec(transactionChain)
                .exec(TransactionCloseActionBuilder(requestName))
        )

    /**
     * Builds full chain of actions that make up a transaction with the default request name.
     *
     * @return Full chain of actions.
     */
    def build(): ChainBuilder = as(genName("tx"))
}

/**
 * Transaction builder: step for transaction parameters.
 *
 * @param params Transaction parameters collected so far.
 */
case class TransactionBuilderParametersStep(params: TransactionParameters) {
    /**
     * Specify transaction timeout.
     *
     * @param timeout Timeout value in milliseconds.
     * @return itself.
     */
    def timeout(timeout: Long): TransactionBuilderParametersStep =
        TransactionBuilderParametersStep(params.copy(timeout = Some(timeout)))

    /**
     * Specify transaction size.
     *
     * @param size Number of entries participating in transaction (may be approximate).
     * @return itself.
     */
    def size(size: Int): TransactionBuilderParametersStep =
        TransactionBuilderParametersStep(params.copy(size = Some(size)))

    /**
     * Specify transaction concurrency.
     *
     * @param concurrency Concurrency.
     * @return itself
     */
    def concurrency(concurrency: TransactionConcurrency): TransactionBuilderParametersStep =
        TransactionBuilderParametersStep(params.copy(concurrency = Some(concurrency)))

    /**
     * Specify transaction isolation.
     *
     * @param isolation Isolation.
     * @return itself
     */
    def isolation(isolation: TransactionIsolation): TransactionBuilderParametersStep =
        TransactionBuilderParametersStep(params.copy(isolation = Some(isolation)))

    /**
     * Specify chain of actions that would be executed within a transaction.
     *
     * @param transactionChain Chain of user actions to be executed within a transaction.
     * @return Full chain of actions.
     */
    def run(transactionChain: ChainBuilder*): TransactionBuilder = {
        if (!params.areValid) {
            throw new IgniteDslInvalidConfigurationException(
                s"Wrong combination of transaction configuration parameters specified: $params"
            )
        }

        if (containsAsyncActions(transactionChain)) {
            throw new IgniteDslInvalidConfigurationException("Async Ignite API can not be used in transaction context")
        }

        TransactionBuilder(params, transactionChain)
    }

    private def containsAsyncActions(transactionChain: Seq[ChainBuilder]): Boolean =
        transactionChain.exists(chainBuilder =>
            chainBuilder.actionBuilders.exists(actionBuilder =>
                actionBuilder match {
                    case builder: CacheActionCommonParameters => builder.withAsync
                    case _                                    => false
                }
            )
        )
}

/**
 * Transaction commit action builder.
 *
 * @param requestName Request name.
 */
case class TransactionCommitActionBuilder(requestName: String = "") extends ActionBuilder {
    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: String): ActionBuilder = this.copy(requestName = requestName)

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    def build(ctx: ScenarioContext, next: Action): Action =
        new TransactionCommitAction(requestName, next, ctx)
}

/**
 * Transaction rollback action builder.
 *
 * @param requestName Request name.
 */
case class TransactionRollbackActionBuilder(requestName: String = "") extends ActionBuilder {
    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: String): ActionBuilder = this.copy(requestName = requestName)

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): TransactionRollbackAction =
        new TransactionRollbackAction(requestName, next, ctx)
}

/**
 * Transaction start action builder.
 *
 * @param requestName Request name.
 * @param params Transaction parameters.
 */
case class TransactionStartActionBuilder(requestName: String, params: TransactionParameters) extends ActionBuilder {
    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new TransactionStartAction(requestName, params, next, ctx)
}

/**
 * Transaction close action builder.
 *
 * @param requestName Request name.
 */
case class TransactionCloseActionBuilder(requestName: String) extends ActionBuilder {
    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new TransactionCloseAction(requestName, next, ctx)
}
