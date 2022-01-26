package com.rarible.protocol.order.core.repository.order

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.dto.Continuation
import com.rarible.protocol.dto.PlatformDto
import com.rarible.protocol.order.core.converters.model.PlatformConverter
import com.rarible.protocol.order.core.misc.div
import com.rarible.protocol.order.core.model.Asset
import com.rarible.protocol.order.core.model.AssetType
import com.rarible.protocol.order.core.model.CollectionAssetType
import com.rarible.protocol.order.core.model.NftAssetType
import com.rarible.protocol.order.core.model.Order
import com.rarible.protocol.order.core.model.OrderFilter
import com.rarible.protocol.order.core.model.OrderFilterAll
import com.rarible.protocol.order.core.model.OrderFilterBidByItem
import com.rarible.protocol.order.core.model.OrderFilterBidByMaker
import com.rarible.protocol.order.core.model.OrderFilterSell
import com.rarible.protocol.order.core.model.OrderFilterSellByCollection
import com.rarible.protocol.order.core.model.OrderFilterSellByItem
import com.rarible.protocol.order.core.model.OrderFilterSellByMaker
import com.rarible.protocol.order.core.model.OrderRaribleV2DataV1
import com.rarible.protocol.order.core.model.OrderStatus
import com.rarible.protocol.order.core.model.Part
import com.rarible.protocol.order.core.model.Platform
import com.rarible.protocol.order.core.model.token
import org.bson.Document
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.and
import org.springframework.data.mongodb.core.query.gt
import org.springframework.data.mongodb.core.query.inValues
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.query.lt
import scalether.domain.Address

object OrderFilterCriteria {
    fun OrderFilter.toCriteria(continuation: String?, limit: Int): Query {
        //for sell filters we sort orders by make price ASC
        //for bid filters we sort orders by take price DESC
        val (criteria, hint) = when (this) {
            is OrderFilterAll -> Criteria().withNoHint()
            is OrderFilterSell -> sell().withHint(withSellHint())
            is OrderFilterSellByItem -> sellByItem(contract, EthUInt256(tokenId), maker, currency).withNoHint()
            is OrderFilterSellByCollection -> sellByCollection(collection).withNoHint()
            is OrderFilterSellByMaker -> sellByMaker(maker).withHint(withSellMakerHint())
            is OrderFilterBidByItem -> bidByItem(contract, EthUInt256(tokenId), maker).withNoHint()
            is OrderFilterBidByMaker -> bidByMaker(maker).withNoHint()
        }

        // TODO scrollTo/getHint/toCritria should be part of Filter
        val query = Query(
            criteria
                .forPlatform(platforms.mapNotNull { convert(it) })
                .scrollTo(continuation, this.sort, this.currency)
                .fromOrigin(origin)
        ).limit(limit).with(sort(this.sort, this.currency))

        if (hint != null) {
            query.withHint(hint)
        }
        this.status?.let {
            if (it.isNotEmpty()) {
                val statuses = it.map { OrderStatus.valueOf(it.name) }.toMutableList()
                if (OrderStatus.INACTIVE in statuses) {
                    statuses += listOf(OrderStatus.ENDED, OrderStatus.NOT_STARTED)
                }
                query.addCriteria(Order::status inValues statuses)
            }
        }
        return query
    }

    private fun sort(sort: OrderFilter.Sort, currency: Address?): Sort {
        return when (sort) {
            OrderFilter.Sort.LAST_UPDATE_DESC -> Sort.by(
                Sort.Direction.DESC,
                Order::lastUpdateAt.name,
                Order::hash.name
            )
            OrderFilter.Sort.LAST_UPDATE_ASC -> Sort.by(
                Sort.Direction.ASC,
                Order::lastUpdateAt.name,
                Order::hash.name
            )
            OrderFilter.Sort.MAKE_PRICE_ASC -> {
                Sort.by(
                    Sort.Direction.ASC,
                    (currency?.let { Order::makePrice } ?: Order::makePriceUsd).name,
                    Order::hash.name
                )
            }
            OrderFilter.Sort.TAKE_PRICE_DESC -> {
                Sort.by(
                    Sort.Direction.DESC,
                    (currency?.let { Order::takePrice } ?: Order::takePriceUsd).name,
                    Order::hash.name
                )
            }
        }
    }

    private fun sellByMaker(maker: Address) =
        sell().and(Order::maker.name).isEqualTo(maker)

    private fun sellByCollection(collection: Address) =
        sell().and("${Order::make.name}.${Asset::type.name}.${NftAssetType::token.name}").isEqualTo(collection)

    private fun sellByItem(token: Address, tokenId: EthUInt256, maker: Address?, currency: Address?) = run {
        var c = tokenCondition(token, tokenId)

        maker?.let { c = c.and(Order::maker.name).`is`(it) }
        currency?.let {
            if (it.equals(Address.ZERO())) { // zero means ETH
                c = c.and(Order::take / Asset::type / AssetType::token).exists(false)
            } else {
                c = c.and(Order::take / Asset::type / AssetType::token).isEqualTo(Address.apply(it))
            }
        }

        c
    }

    private fun tokenCondition(token: Address, tokenId: EthUInt256): Criteria {
        return Criteria().andOperator(
            Order::make / Asset::type / NftAssetType::token isEqualTo token,
            Criteria().orOperator(
                Order::make / Asset::type / NftAssetType::tokenId isEqualTo tokenId,
                Criteria("${Order::make.name}.${Asset::type.name}.${NftAssetType::tokenId.name}").exists(false)
                    .and("${Order::make.name}.${Asset::type.name}._class").isEqualTo(CollectionAssetType::class.java.name)
            )
        )
    }

    private fun sell() =
        Criteria.where("${Order::make.name}.${Asset::type.name}.${AssetType::nft.name}").isEqualTo(true)

    private fun bidByItem(token: Address, tokenId: EthUInt256, maker: Address?) = run {
        val criteria = Criteria().orOperator(
            Criteria.where("${Order::take.name}.${Asset::type.name}.${NftAssetType::token.name}").isEqualTo(token)
                .and("${Order::take.name}.${Asset::type.name}.${NftAssetType::tokenId.name}").isEqualTo(tokenId),
            Criteria.where("${Order::take.name}.${Asset::type.name}._class").isEqualTo(CollectionAssetType::class.java.name)
                .and("${Order::take.name}.${Asset::type.name}.${NftAssetType::token.name}").isEqualTo(token)
        )

        if (maker != null) {
            criteria.and(Order::maker.name).isEqualTo(maker)
        } else {
            criteria
        }
    }

    private fun bidByMaker(maker: Address) =
        Criteria()
            .and(Order::maker.name).isEqualTo(maker)
            .and("${Order::take.name}.${Asset::type.name}.${AssetType::nft.name}").isEqualTo(true)

    private infix fun Criteria.fromOrigin(origin: Address?) = origin?.let {
        and("${Order::data.name}.${OrderRaribleV2DataV1::originFees.name}")
            .elemMatch(Criteria.where(Part::account.name).`is`(origin))
    } ?: this

    private infix fun Criteria.forPlatform(platforms: List<Platform>): Criteria {
        return if (platforms.isEmpty()) {
            this
        } else {
            and(Order::platform).inValues(platforms)
        }
    }

    private fun Criteria.scrollTo(continuation: String?, sort: OrderFilter.Sort, currency: Address?) =
        when (sort) {
            OrderFilter.Sort.LAST_UPDATE_DESC -> {
                val lastDate = Continuation.parse<Continuation.LastDate>(continuation)
                lastDate?.let { c ->
                    this.orOperator(
                        Order::lastUpdateAt lt c.afterDate,
                        Criteria().andOperator(
                            Order::lastUpdateAt isEqualTo c.afterDate,
                            Order::hash lt c.afterId
                        )
                    )
                } ?: this
            }
            OrderFilter.Sort.LAST_UPDATE_ASC -> {
                val lastDate = Continuation.parse<Continuation.LastDate>(continuation)
                lastDate?.let { c ->
                    this.orOperator(
                        Order::lastUpdateAt gt c.afterDate,
                        Criteria().andOperator(
                            Order::lastUpdateAt isEqualTo c.afterDate,
                            Order::hash gt c.afterId
                        )
                    )
                } ?: this
            }
            OrderFilter.Sort.TAKE_PRICE_DESC -> {
                val price = Continuation.parse<Continuation.Price>(continuation)
                price?.let { c ->
                    if (currency != null) {
                        this.orOperator(
                            Order::takePrice lt c.afterPrice,
                            Criteria().andOperator(
                                Order::takePrice isEqualTo c.afterPrice,
                                Order::hash lt c.afterId
                            )
                        )
                    } else {
                        this.orOperator(
                            Order::takePriceUsd lt c.afterPrice,
                            Criteria().andOperator(
                                Order::takePriceUsd isEqualTo c.afterPrice,
                                Order::hash lt c.afterId
                            )
                        )
                    }
                } ?: this
            }
            OrderFilter.Sort.MAKE_PRICE_ASC -> {
                val price = Continuation.parse<Continuation.Price>(continuation)
                price?.let { c ->
                    if (currency != null) {
                        this.orOperator(
                            Order::makePrice gt c.afterPrice,
                            Criteria().andOperator(
                                Order::makePrice isEqualTo c.afterPrice,
                                Order::hash gt c.afterId
                            )
                        )
                    } else { // Deprecated
                        this.orOperator(
                            Order::makePriceUsd gt c.afterPrice,
                            Criteria().andOperator(
                                Order::makePriceUsd isEqualTo c.afterPrice,
                                Order::hash gt c.afterId
                            )
                        )
                    }
                } ?: this
            }
        }

    private fun convert(platform: PlatformDto): Platform? = PlatformConverter.convert(platform)

    private infix fun Criteria.withHint(index: Document) = Pair(this, index)
    private fun Criteria.withNoHint() = Pair<Criteria, Document?>(this, null)

    private fun OrderFilterSell.withSellHint(): Document {
        val platforms = this.platforms.mapNotNull { convert(it) }
        val statuses = this.status

        val singlePlatform = platforms.size == 1
        val singleStatus = statuses?.size == 1

        return when {
            singlePlatform && singleStatus -> OrderRepositoryIndexes.SELL_ORDERS_PLATFORM_STATUS_DEFINITION.indexKeys
            singlePlatform -> OrderRepositoryIndexes.SELL_ORDERS_PLATFORM_DEFINITION.indexKeys
            singleStatus -> OrderRepositoryIndexes.SELL_ORDERS_STATUS_DEFINITION.indexKeys
            else -> OrderRepositoryIndexes.SELL_ORDERS_DEFINITION.indexKeys
        }
    }

    private fun OrderFilterSellByMaker.withSellMakerHint(): Document {
        return when {
            platforms.isEmpty() -> OrderRepositoryIndexes.SELL_ORDERS_BY_MAKER_DEFINITION.indexKeys
            else -> OrderRepositoryIndexes.SELL_ORDERS_BY_MAKER_PLATFORM_DEFINITION.indexKeys
        }
    }
}
