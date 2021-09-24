package com.rarible.protocol.order.listener.service.order

import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.repository.order.OrderVersionRepository
import com.rarible.protocol.order.core.service.PriceUpdateService
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class OrderPriceUpdateService(
    private val orderRepository: OrderRepository,
    private val orderVersionRepository: OrderVersionRepository,
    private val priceUpdateService: PriceUpdateService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Update order's USD prices without calling the OrderReduceService.
     */
    suspend fun updateOrderPrice(hash: Word, at: Instant) {
        updateOrderVersionPrice(hash, at)
        try {
            val order = orderRepository.findById(hash) ?: return
            val usdValue = priceUpdateService.getAssetsUsdValue(order.make, order.take, at)
            if (usdValue != null) {
                orderRepository.save(order.withOrderUsdValue(usdValue))
            }
        } catch (e: Exception) {
            logger.error("Failed to update prices of order $hash", e)
        }
    }

    suspend fun updateOrderVersionPrice(hash: Word, at: Instant) {
        orderVersionRepository.findAllByHash(hash).collect { version ->
            val usdPrice = priceUpdateService.getAssetsUsdValue(make = version.make, take = version.take, at = at)
            if (usdPrice != null) {
                try {
                    version.withOrderUsdValue(usdPrice)
                        .let { orderVersionRepository.save(it) }
                        .awaitFirst()
                } catch (ex: Exception) {
                    logger.error("Can't update prices for order version ${version.id}", ex)
                }
            }
        }
    }
}
