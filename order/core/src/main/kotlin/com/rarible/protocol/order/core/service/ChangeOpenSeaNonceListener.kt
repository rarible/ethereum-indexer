package com.rarible.protocol.order.core.service

import com.rarible.protocol.order.core.configuration.OrderIndexerProperties
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.service.block.ChangeNonceListener
import kotlinx.coroutines.flow.collect
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class ChangeOpenSeaNonceListener(
    private val orderRepository: OrderRepository,
    private val orderUpdateService: OrderUpdateService,
    private val properties: OrderIndexerProperties
) : ChangeNonceListener {

    override suspend fun onNewMakerNonce(maker: Address, newNonce: Long) {
        val fixedMewNonce = newNonce + properties.openSeaNonceIncrement

        require(fixedMewNonce > 0) {
            "Maker $maker nonce is less then zero $fixedMewNonce"
        }
        logger.info("New OpenSea nonce $fixedMewNonce detected for maker $maker")
        orderRepository
            .findOpenSeaHashesByMakerAndByNonce(maker, fromIncluding = fixedMewNonce - 1,  toExcluding = fixedMewNonce)
            .collect { hash ->
                orderUpdateService.update(hash)
            }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ChangeOpenSeaNonceListener::class.java)
    }
}

