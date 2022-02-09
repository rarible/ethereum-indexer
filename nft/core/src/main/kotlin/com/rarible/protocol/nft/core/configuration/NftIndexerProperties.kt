package com.rarible.protocol.nft.core.configuration

import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.ethereum.domain.Blockchain
import com.rarible.protocol.nft.core.model.FeatureFlags
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

const val RARIBLE_PROTOCOL_NFT_INDEXER = "common"

@ConstructorBinding
@ConfigurationProperties(RARIBLE_PROTOCOL_NFT_INDEXER)
data class NftIndexerProperties(
    val basePublicApiUrl: String,
    val kafkaReplicaSet: String,
    val blockchain: Blockchain,
    val metricRootPath: String,
    val maxPollRecords: Int = 100,
    var cryptoPunksContractAddress: String,
    var openseaLazyMintAddress: String,
    var royaltyRegistryAddress: String,
    val factory: FactoryAddresses,
    val nftItemMetaExtenderWorkersCount: Int = 4,
    val daemonWorkerProperties: DaemonWorkerProperties = DaemonWorkerProperties(),
    val featureFlags: FeatureFlags = FeatureFlags(),
    val nftCollectionMetaExtenderWorkersCount: Int = 4,
    val confirmationBlocks: Int = 12,
    val ownershipSaveBatch: Int = 20,
    val returnOnlyCacheItemMeta: Boolean = false,
    val reduceProperties: ReduceProperties = ReduceProperties(),
    val scannerProperties: ScannerProperties = ScannerProperties(),
    val itemMeta: ItemMetaProperties = ItemMetaProperties()
) {
    data class ScannerProperties(
        val skipTransferContractTokens: List<String> = emptyList()
    )

    data class ReduceProperties(
        val skipOwnerships: Boolean = false
    )

    data class FactoryAddresses(
        val erc721Rarible: String,
        val erc721RaribleUser: String,
        val erc1155Rarible: String,
        val erc1155RaribleUser: String
    )

    data class ItemMetaProperties(
        val maxNameLength: Int = 1000,
        val maxDescriptionLength: Int = 10000
    )

}
