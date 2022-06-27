package com.rarible.protocol.order.core.configuration

import com.rarible.ethereum.domain.Blockchain
import com.rarible.protocol.order.core.model.NodeType
import io.daonomic.rpc.domain.Binary
import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.NestedConfigurationProperty
import scalether.domain.Address

const val RARIBLE_PROTOCOL_ORDER_INDEXER = "common"

@ConstructorBinding
@ConfigurationProperties(RARIBLE_PROTOCOL_ORDER_INDEXER)
data class OrderIndexerProperties(
    val blockchain: Blockchain,
    val kafkaReplicaSet: String,
    val eip712DomainName: String,
    val eip712DomainVersion: String,
    val openseaEip712DomainName: String,
    val openseaEip712DomainVersion: String,
    var openSeaNonceIncrement: Long = 0,
    val chainId: Int,
    var operatorPrivateKey: Binary,
    val protocolCommission: Int,
    val metricRootPath: String,
    @NestedConfigurationProperty
    val exchangeContractAddresses: ExchangeContractAddresses,
    @NestedConfigurationProperty
    val auctionContractAddresses: AuctionContractAddresses,
    @NestedConfigurationProperty
    val transferProxyAddresses: TransferProxyAddresses,
    @NestedConfigurationProperty
    val publish: PublishProperties = PublishProperties(),
    @NestedConfigurationProperty
    val featureFlags: FeatureFlags = FeatureFlags(),
    val blockCountBeforeSnapshot: Int = 12,
    val nodeType: NodeType?,
    @NestedConfigurationProperty
    val raribleOrderExpiration: RaribleOrderExpirationProperties = RaribleOrderExpirationProperties()
) {
    data class ExchangeContractAddresses(
        var v1: Address,
        var v1Old: Address? = null,
        var v2: Address,
        var openSeaV1: Address,
        var openSeaV2: Address,
        var cryptoPunks: Address,
        var zeroEx: Address,
    )

    data class AuctionContractAddresses(
        var v1: Address
    )

    data class TransferProxyAddresses(
        val transferProxy: Address,
        val erc20TransferProxy: Address,
        val erc721LazyTransferProxy: Address,
        val erc1155LazyTransferProxy: Address,
        var cryptoPunksTransferProxy: Address
    )

    data class PublishProperties(
        val publishOpenSeaOrdersToCommonTopic: Boolean = false,
        val publishAuctionActivity: Boolean = false
    )

    data class FeatureFlags(
        val showAllOrdersByDefault: Boolean = false,
        val showOpenSeaOrdersWithOtherPlatforms: Boolean = false,
        val hideOpenSeaSignatures: Boolean = false,
        val hideInactiveOrders: Boolean = false,
        val maxOpenSeaNonceCalculation: Int = 10,
        val skipGetTrace: Boolean = false
    )

    data class RaribleOrderExpirationProperties(
        val bidExpirePeriod: Duration = Duration.ofDays(60L),
        val delayPeriod: Duration = Duration.ofMinutes(10L)
    )
}
