package com.rarible.protocol.nft.core.service

import com.rarible.core.cache.CacheService
import com.rarible.core.cache.get
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.contracts.external.royalties.IRoyaltiesProvider
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.ItemProperties
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.model.Royalty
import com.rarible.protocol.nft.core.repository.RoyaltyRepository
import com.rarible.protocol.nft.core.service.item.meta.ItemPropertiesService
import com.rarible.protocol.nft.core.service.item.meta.descriptors.RoyaltyCacheDescriptor
import io.daonomic.rpc.RpcCodeException
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import scalether.domain.Address
import scalether.transaction.MonoTransactionSender
import java.math.BigInteger

@Service
class RoyaltyService(
    private val sender: MonoTransactionSender,
    private val nftIndexerProperties: NftIndexerProperties,
    private val royaltyRepository: RoyaltyRepository,
    private val royaltyCacheDescriptor: RoyaltyCacheDescriptor,
    private val cacheService: CacheService
) {

    // TODO: handle the two cases differently:
    //  1) royalties are not yet set for the item (this is the case while the item hasn't been minted yet - pending transaction)
    //  2) item doesn't have any royalties at all
    //  Currently, we request royalties from the contract in both cases.
    suspend fun getRoyaltyDeprecated(address: Address, tokenId: EthUInt256): List<Part> {
        val cachedRoyalties = royaltyRepository.findByTokenAndId(address, tokenId).awaitFirstOrNull()
        if (cachedRoyalties != null && cachedRoyalties.royalty.isNotEmpty()) {
            return cachedRoyalties.royalty
        }
        logger.info("Requesting royalties $address:$tokenId")
        val royalties = getFromContract(address, tokenId)
        if (royalties.isNotEmpty()) {
            return royaltyRepository.save(
                Royalty(
                    address = address,
                    tokenId = tokenId,
                    royalty = royalties
                )
            ).awaitSingle().royalty
        }
        return emptyList()
    }

    suspend fun getRoyalty(itemId: ItemId): List<Part> {
        return cacheService
            .get(itemId.toString(), royaltyCacheDescriptor, true)
            .awaitSingle()
    }

    suspend fun getFromContract(address: Address, tokenId: EthUInt256): List<Part> = try {
        val provider = IRoyaltiesProvider(Address.apply(nftIndexerProperties.royaltyRegistryAddress), sender)
        provider.getRoyalties(address, tokenId.value)
            .call().awaitSingle()
            .map { Part(it._1, it._2.intValueExact()) }.toList()
            .also { logger.info("Got royalties for $address:$tokenId: $it") }
    } catch (e: RpcCodeException) {
        logger.info(
            "RoyaltiesProvider does not know about royalties for $address:$tokenId, see Jira RPC-109, " +
                    "returned ${e.message()}"
        )
        emptyList()
    } catch (e: Exception) {
        logger.error("Failed to request royalties for $address:$tokenId", e)
        emptyList()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RoyaltyService::class.java)
    }
}
