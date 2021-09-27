package com.rarible.protocol.order.core.repository.order

import com.mongodb.client.result.UpdateResult
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.order.core.misc.div
import com.rarible.protocol.order.core.model.*
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.*
import org.springframework.data.mongodb.core.query.*
import reactor.core.publisher.Mono
import scalether.domain.Address
import java.time.Instant
import java.util.*

class MongoOrderRepository(
    private val template: ReactiveMongoOperations
) : OrderRepository {

    private val logger = LoggerFactory.getLogger(javaClass)

    override suspend fun dropIndexes() {
        dropIndexes(
            "make.type.nft_1_lastUpdateAt_1__id_1",
            "make.type.token_1_make.type.tokenId_1_lastUpdateAt_1__id_1"
        )
    }

    override suspend fun createIndexes() {
        OrderRepositoryIndexes.ALL_INDEXES.forEach { index ->
            template.indexOps(COLLECTION).ensureIndex(index).awaitFirst()
        }
    }

    override suspend fun save(order: Order): Order {
        return template.save(order).awaitFirst()
    }

    override suspend fun findById(hash: Word): Order? {
        return template.findById<Order>(hash).awaitFirstOrNull()
    }

    override fun findAll(hashes: Collection<Word>): Flow<Order> {
        val criteria = Criteria.where("_id").inValues(hashes)
        return template.find<Order>(Query.query(criteria)).asFlow()
    }

    override suspend fun search(query: Query): List<Order> {
        return template.query<Order>()
            .matching(query)
            .all()
            .collectList()
            .awaitFirst()
    }

    override suspend fun search(filter: OrderFilter): List<Order> {
        val limit = filter.limit
        val hint = filter.hint
        val sort = filter.dataSort
        val criteria = filter.getCriteria()

        val query = Query(criteria)

        if (hint != null) {
            query.withHint(hint)
        }
        query.with(sort).limit(limit)
        return template.find<Order>(query).collectList().awaitFirst()
    }

    override suspend fun remove(hash: Word): Boolean {
        val criteria = Criteria.where("_id").isEqualTo(hash)
        return template.remove(Query(criteria), Order::class.java).awaitFirst().deletedCount > 0
    }

    override fun findActive(): Flow<Order> {
        return template.query<Order>().matching(
            Query(
                Criteria().andOperator(
                    Order::cancelled isEqualTo false,
                    Order::makeStock ne EthUInt256.ZERO
                )
            )
        ).all().asFlow()
    }

    override fun findAll(): Flow<Order> {
        return template.findAll<Order>().asFlow()
    }

    override fun findByTargetNftAndNotCanceled(maker: Address, token: Address, tokenId: EthUInt256): Flow<Order> {
        val criteria =
            (Order::make / Asset::type / NftAssetType::token isEqualTo token)
                .and(Order::make / Asset::type / NftAssetType::tokenId).isEqualTo(tokenId)
                .and(Order::maker).isEqualTo(maker)
                .and(Order::cancelled).isEqualTo(false)

        val query = Query(criteria)
        return template.query<Order>().matching(query).all().asFlow()
    }

    override fun findByTargetBalanceAndNotCanceled(maker: Address, token: Address): Flow<Order> {
        val criteria = Criteria.where("${Order::make.name}.${Asset::type.name}.${AssetType::nft.name}").isEqualTo(false)
            .and(Order::maker.name).isEqualTo(maker)
            .and(Order::cancelled.name).isEqualTo(false)
            .and("${Order::make.name}.${Asset::type.name}.${Erc20AssetType::token.name}").isEqualTo(token)

        val query = Query(criteria)
        return template.query<Order>().matching(query).all().asFlow()
    }

    override fun findAllBeforeLastUpdateAt(lastUpdatedAt: Date?): Flow<Order> {
        val criteria = Criteria()
            .run { lastUpdatedAt?.let { and(Order::lastUpdateAt).lte(it) } ?: this }
            .run { and(Order::cancelled).ne(true) }

        val queue = Query().addCriteria(criteria)

        queue.with(Sort.by(Sort.Direction.DESC, Order::lastUpdateAt.name))
        return template.query<Order>().matching(queue).all().asFlow()
    }

    fun resetMakeStockAfter(now: Instant): Mono<UpdateResult> {
        val query = Query(
            Criteria().andOperator(
                Order::end exists true,
                Order::end lt now.toEpochMilli()
            )
        )
        return template.updateFirst(query, Update().set(Order::makeStock.name, EthUInt256.ZERO), Order::class.java)
    }

    fun findActualZeroMakeStock(now: Instant): Flow<Order> {
        val query = Query(
            Criteria().andOperator(
                Order::start exists true,
                Order::end exists true,
                Order::start lte now.toEpochMilli(),
                Order::end gte now.toEpochMilli(),
                Order::makeStock isEqualTo EthUInt256.ZERO
            )
        )
        return template.query<Order>().matching(query).all().asFlow()
    }

    private suspend fun dropIndexes(vararg names: String) {
        val existing = template.indexOps(COLLECTION).indexInfo.map { it.name }.collectList().awaitFirst()
        for (name in names) {
            if (existing.contains(name)) {
                logger.info("dropping index $name")
                template.indexOps(COLLECTION).dropIndex(name).awaitFirstOrNull()
            } else {
                logger.info("skipping drop index $name")
            }
        }
    }

    companion object {
        const val COLLECTION = "order"
    }
}
