package com.rarible.protocol.order.listener.job

import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.sequential.SequentialDaemonWorker
import com.rarible.protocol.order.core.configuration.OrderIndexerProperties
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.service.OrderReduceService
import com.rarible.protocol.order.listener.configuration.OrderListenerProperties
import io.micrometer.core.instrument.MeterRegistry
import java.time.Instant
import kotlin.math.exp
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow

class RaribleBidsCanceledAfterExpiredJob(
    private val orderRepository: OrderRepository,
    private val orderReduceService: OrderReduceService,
    raribleOrderExpiration: OrderIndexerProperties.RaribleOrderExpirationProperties,
    properties: OrderListenerProperties,
    meterRegistry: MeterRegistry
) : SequentialDaemonWorker(
    meterRegistry,
    DaemonWorkerProperties(pollingPeriod = properties.raribleExpiredBidWorker.pollingPeriod)
) {
    private val expirePeriod = raribleOrderExpiration.bidExpirePeriod

    private val delayPeriod = raribleOrderExpiration.delayPeriod

    override suspend fun handle() {
        logger.info("Start MakeBidCanceledAfterExpiredJob")
        try {
            val expired = orderRepository.findAllLiveBidsHashesLastUpdatedBefore(Instant.now() - expirePeriod).toList()
            if (expired.isEmpty()) {
                delay(delayPeriod.toMillis())
            } else {
                expired.forEach{
                    orderReduceService.update(it).asFlow().collect { bid ->
                        logger.info("Expire bid $bid after $expirePeriod, bid is cancelled now!")
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Unable to handle expired bids: ${e.message}", e)
            throw e
        }
    }
}
