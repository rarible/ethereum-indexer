package com.rarible.protocol.nftorder.listener.service

import com.rarible.core.client.WebClientResponseProxyException
import com.rarible.protocol.dto.*
import com.rarible.protocol.nftorder.core.model.ItemId
import com.rarible.protocol.nftorder.core.model.OwnershipId
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class OrderEventService(
    private val itemEventService: ItemEventService,
    private val ownershipEventService: OwnershipEventService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun updateOrder(order: OrderDto) = coroutineScope {

        val makeItemId = toItemId(order.make.assetType)
        val takeItemId = toItemId(order.take.assetType)

        val mFuture = makeItemId?.let {
            async { ignoreApi404 { itemEventService.onItemBestSellOrderUpdated(makeItemId, order) } }
        }
        val tFuture = takeItemId?.let {
            async { ignoreApi404 { itemEventService.onItemBestBidOrderUpdated(takeItemId, order) } }
        }
        val oFuture = makeItemId?.let {
            val ownershipId = OwnershipId(makeItemId.token, makeItemId.tokenId, order.maker)
            async { ignoreApi404 { ownershipEventService.onOwnershipBestSellOrderUpdated(ownershipId, order) } }
        }

        mFuture?.await()
        tFuture?.await()
        oFuture?.await()
    }

    private fun toItemId(assetType: AssetTypeDto): ItemId? {

        return when (assetType) {
            is Erc721AssetTypeDto -> ItemId.of(assetType.contract, assetType.tokenId)
            is Erc1155AssetTypeDto -> ItemId.of(assetType.contract, assetType.tokenId)
            is Erc721LazyAssetTypeDto -> ItemId.of(assetType.contract, assetType.tokenId)
            is Erc1155LazyAssetTypeDto -> ItemId.of(assetType.contract, assetType.tokenId)
            is CryptoPunksAssetTypeDto -> ItemId.of(assetType.contract, assetType.punkId.toBigInteger())
            is GenerativeArtAssetTypeDto -> null
            is EthAssetTypeDto -> null
            is Erc20AssetTypeDto -> null
        }
    }

    private suspend fun ignoreApi404(call: suspend () -> Unit) {
        try {
            call()
        } catch (ex: WebClientResponseProxyException) {
            logger.warn("Received NOT_FOUND code from client, details: {}, message: {}", ex.data, ex.message)
        }
    }

}