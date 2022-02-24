package com.rarible.protocol.order.listener.service.order

import com.rarible.core.task.TaskHandler
import com.rarible.protocol.order.core.configuration.OrderIndexerProperties
import com.rarible.protocol.order.core.model.Order
import com.rarible.protocol.order.core.model.OrderOpenSeaV1DataV1
import com.rarible.protocol.order.core.model.OrderStatus
import com.rarible.protocol.order.core.model.Platform
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.service.OrderReduceService
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class RemoveOpenSeaOutdatedOrdersTaskHandler(
    private val orderRepository: OrderRepository,
    private val orderReduceService: OrderReduceService,
    private val exchangeContractAddresses: OrderIndexerProperties.ExchangeContractAddresses,
) : TaskHandler<String> {

    val logger: Logger = LoggerFactory.getLogger(RemoveOpenSeaOutdatedOrdersTaskHandler::class.java)

    override val type: String
        get() = REMOVE_OPEN_SEA_OUTDATED_ORDERS

    override fun runLongTask(from: String?, param: String): Flow<String> {
        val queryStatuses = setOf(
            OrderStatus.NOT_STARTED,
            OrderStatus.ACTIVE,
            OrderStatus.INACTIVE
        )

        val orders =  orderRepository.findAll(Platform.OPEN_SEA, queryStatuses).filter { isExchangeOpenSea(it) }
            .map { updateOrder(it) }

        return orders
    }

    private fun isExchangeOpenSea(order: Order): Boolean {
        return (order.data as? OrderOpenSeaV1DataV1)?.exchange == exchangeContractAddresses.openSeaV1
    }

    private suspend fun updateOrder(order: Order): String  {
        if (orderReduceService.updateOrder(order.hash) != null) {
            logger.info("OpenSea order ${order.hash} has been updated.")
        }

        return order.hash.toString()
    }

    companion object {
        const val REMOVE_OPEN_SEA_OUTDATED_ORDERS = "REMOVE_OPEN_SEA_OUTDATED_ORDERS"
    }
}