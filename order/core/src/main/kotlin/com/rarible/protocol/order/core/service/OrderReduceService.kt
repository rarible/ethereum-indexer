package com.rarible.protocol.order.core.service

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.listener.log.domain.EventData
import com.rarible.ethereum.listener.log.domain.LogEvent
import com.rarible.ethereum.listener.log.domain.LogEventStatus
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.provider.ProtocolCommissionProvider
import com.rarible.protocol.order.core.repository.exchange.ExchangeHistoryRepository
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.repository.order.OrderVersionRepository
import com.rarible.protocol.order.core.service.asset.AssetBalanceProvider
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import scalether.domain.Address
import java.time.Instant

@Component
class OrderReduceService(
    private val exchangeHistoryRepository: ExchangeHistoryRepository,
    private val orderRepository: OrderRepository,
    private val orderVersionRepository: OrderVersionRepository,
    private val assetBalanceProvider: AssetBalanceProvider,
    private val protocolCommissionProvider: ProtocolCommissionProvider,
    private val priceNormalizer: PriceNormalizer,
    private val priceUpdateService: PriceUpdateService
) {

    suspend fun updateOrderMakeStock(orderHash: Word, knownMakeBalance: EthUInt256? = null): Order {
        val order = update(orderHash = orderHash).awaitSingle()
        val withMakeStock = order.withUpdatedMakeStock(knownMakeBalance)
        val updated = if (order.makeStock == EthUInt256.ZERO && withMakeStock.makeStock != EthUInt256.ZERO) {
            priceUpdateService.updateOrderPrice(withMakeStock, Instant.now())
        } else {
            withMakeStock
        }
        val saved = orderRepository.save(updated)
        logger.info("Updated order $orderHash, makeStock=${saved.makeStock}")
        return saved
    }

    fun update(orderHash: Word? = null, fromOrderHash: Word? = null): Flux<Order> {
        logger.info("Update hash=$orderHash fromHash=$fromOrderHash")
        return Flux.mergeOrdered(
            compareBy<OrderUpdate, Word>(wordComparator) { it.orderHash },
            orderVersionRepository.findAllByHash(orderHash, fromOrderHash).map { OrderUpdate.ByOrderVersion(it) },
            exchangeHistoryRepository.findLogEvents(orderHash, fromOrderHash).map { OrderUpdate.ByLogEvent(it) }
        )
            .windowUntilChanged { it.orderHash }
            .concatMap {
                it.switchOnFirst { firstSignal, updates ->
                    val firstUpdate = firstSignal.get()
                    if (firstUpdate != null) {
                        updateOrder(updates)
                    } else {
                        Mono.empty()
                    }
                }
            }
    }

    private sealed class OrderUpdate {
        abstract val orderHash: Word

        data class ByOrderVersion(val orderVersion: OrderVersion) : OrderUpdate() {
            override val orderHash get() = orderVersion.hash
        }

        data class ByLogEvent(val logEvent: LogEvent) : OrderUpdate() {
            override val orderHash get() = logEvent.data.toExchangeHistory().hash
        }
    }

    private fun updateOrder(updates: Flux<OrderUpdate>): Mono<Order> = mono {
        val order = updates.asFlow().fold(emptyOrder) { order, update ->
            when (update) {
                is OrderUpdate.ByOrderVersion -> order.updateWith(update.orderVersion)
                is OrderUpdate.ByLogEvent -> order.updateWith(
                    update.logEvent.status,
                    update.logEvent.data.toExchangeHistory()
                )
            }
        }
        updateOrderWithState(order)
    }

    private fun Order.updateWith(logEventStatus: LogEventStatus, orderExchangeHistory: OrderExchangeHistory): Order {
        return when (logEventStatus) {
            LogEventStatus.PENDING -> copy(pending = pending + orderExchangeHistory)
            LogEventStatus.CONFIRMED -> when (orderExchangeHistory) {
                is OrderSideMatch -> copy(
                    fill = fill.plus(orderExchangeHistory.fill),
                    lastUpdateAt = maxOf(lastUpdateAt, orderExchangeHistory.date)
                )
                is OrderCancel -> copy(
                    cancelled = true,
                    lastUpdateAt = maxOf(lastUpdateAt, orderExchangeHistory.date)
                )
            }
            else -> this
        }
    }

    private suspend fun Order.updateWith(orderVersion: OrderVersion): Order = copy(
        make = orderVersion.make,
        take = orderVersion.take,
        signature = orderVersion.signature,
        lastUpdateAt = orderVersion.createdAt,
        priceHistory = getUpdatedPriceHistoryRecords(this, orderVersion)
    )

    private suspend fun getUpdatedPriceHistoryRecords(previous: Order, orderVersion: OrderVersion): List<OrderPriceHistoryRecord> {
        if (previous.make == orderVersion.make && previous.take == orderVersion.take) {
            return previous.priceHistory
        }
        val newRecord = OrderPriceHistoryRecord(
            orderVersion.createdAt,
            priceNormalizer.normalize(orderVersion.make),
            priceNormalizer.normalize(orderVersion.take)
        )
        return (previous.priceHistory + listOf(newRecord)).sortedByDescending { it.date }.take(Order.MAX_PRICE_HISTORIES)
    }

    private suspend fun Order.withUpdatedMakeStock(knownMakeBalance: EthUInt256? = null): Order {
        val makeBalance = knownMakeBalance ?: assetBalanceProvider.getAssetStock(maker, make.type) ?: EthUInt256.ZERO
        return withMakeBalance(makeBalance, protocolCommissionProvider.get())
    }

    private suspend fun Order.withNewPrice(): Order {
        val orderUsdValue = priceUpdateService.getAssetsUsdValue(make, take, nowMillis())
        return if (orderUsdValue != null) withOrderUsdValue(orderUsdValue) else this
    }

    private suspend fun updateOrderWithState(order0: Order): Order {
        val order = order0
            .withUpdatedMakeStock()
            .withNewPrice()
        val version = orderRepository.findById(order.hash)?.version
        val saved = orderRepository.save(order.copy(version = version))
        logger.info(buildString {
            append("Updated order: ")
            append("hash=${saved.hash}, ")
            append("makeStock=${saved.makeStock}, ")
            append("fill=${saved.fill}, ")
            append("cancelled=${saved.cancelled}")
        })
        return saved
    }

    companion object {
        private val emptyOrder = Order(
            maker = Address.ZERO(),
            taker = Address.ZERO(),
            make = Asset(EthAssetType, EthUInt256.ZERO),
            take = Asset(EthAssetType, EthUInt256.ZERO),
            type = OrderType.RARIBLE_V2,
            fill = EthUInt256.ZERO,
            cancelled = false,
            makeStock = EthUInt256.ZERO,
            salt = EthUInt256.ZERO,
            start = null,
            end = null,
            data = OrderRaribleV2DataV1(emptyList(), emptyList()),
            signature = null,
            createdAt = Instant.EPOCH,
            lastUpdateAt = Instant.EPOCH,
            pending = emptyList(),
            makePriceUsd = null,
            takePriceUsd = null,
            makeUsd = null,
            takeUsd = null,
            priceHistory = emptyList(),
            platform = Platform.RARIBLE
        )

        private val wordComparator = Comparator<Word> r@{ w1, w2 ->
            val w1Bytes = w1.bytes()
            val w2Bytes = w2.bytes()
            for (i in 0 until minOf(w1Bytes.size, w2Bytes.size)) {
                if (w1Bytes[i] != w2Bytes[i]) {
                    return@r w1Bytes[i].compareTo(w2Bytes[i])
                }
            }
            return@r w1Bytes.size.compareTo(w2Bytes.size)
        }

        val logger: Logger = LoggerFactory.getLogger(OrderReduceService::class.java)

        fun EventData.toExchangeHistory(): OrderExchangeHistory =
            requireNotNull(this as? OrderExchangeHistory) { "Unexpected exchange history type ${this::class}" }
    }
}
