package com.rarible.protocol.order.listener.configuration

import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

internal const val RARIBLE_PROTOCOL_LISTENER = "listener"

@ConstructorBinding
@ConfigurationProperties(RARIBLE_PROTOCOL_LISTENER)
class OrderListenerProperties(
    val monitoringWorker: DaemonWorkerProperties = DaemonWorkerProperties(),
    val publishTaskDelayMs: Long = 1000L,
    val loadOpenSeaOrders: Boolean = false,
    val loadOpenSeaPeriod: Duration = Duration.ofSeconds(1),
    val loadOpenSeaDelay: Duration = Duration.ofSeconds(5),
    val saveOpenSeaOrdersBatchSize: Int = 200,
    val openSeaOrderSide: OrderSide? = null,
    val updateStatusByStartEndEnabled: Boolean = false,
    val openSeaClientUserAgents: String = ""
) {
    enum class OrderSide {
        ALL,
        SELL,
        BID
    }
}
