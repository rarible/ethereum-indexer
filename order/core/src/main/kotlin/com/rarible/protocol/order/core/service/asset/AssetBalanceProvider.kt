package com.rarible.protocol.order.core.service.asset

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.order.core.misc.ownershipId
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.service.balance.BalanceControllerApiService
import com.rarible.protocol.order.core.service.balance.EthBalanceService
import com.rarible.protocol.order.core.service.nft.NftOwnershipApiService
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class AssetBalanceProvider(
    private val erc20BalanceApi: BalanceControllerApiService,
    private val nftOwnershipApi: NftOwnershipApiService,
    private val ethBalanceService: EthBalanceService
) {
    suspend fun getAssetStock(owner: Address, type: AssetType): EthUInt256? {
        return when (type) {
            is Erc20AssetType -> {
                erc20BalanceApi
                    .getBalance(type.token, owner)
                    ?.let { balance -> EthUInt256.of(balance.balance) }
            }
            is Erc721AssetType, is Erc1155AssetType, is CryptoPunksAssetType -> {
                val ownershipId = type.ownershipId(owner)
                nftOwnershipApi
                    .getOwnershipById(ownershipId)
                    ?.let { ownership -> EthUInt256.of(ownership.value) }
            }
            is Erc721LazyAssetType -> {
                EthUInt256.ONE
            }
            is GenerativeArtAssetType -> {
                EthUInt256.of(Long.MAX_VALUE)
            }
            is CollectionAssetType -> {
                EthUInt256.of(Long.MAX_VALUE)
            }
            is Erc1155LazyAssetType -> {
                type.supply
            }
            is EthAssetType -> {
                ethBalanceService.getBalance(owner)
            }
        }
    }
}
