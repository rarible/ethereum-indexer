package com.rarible.protocol.nft.core.service.item.meta.descriptors

import com.rarible.core.apm.CaptureSpan
import com.rarible.protocol.contracts.erc1155.v1.rarible.RaribleToken
import com.rarible.protocol.contracts.erc721.v4.rarible.MintableToken
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.Token
import com.rarible.protocol.nft.core.model.TokenStandard
import com.rarible.protocol.nft.core.repository.TokenRepository
import com.rarible.protocol.nft.core.service.item.meta.logMetaLoading
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import scalether.transaction.MonoTransactionSender
import java.time.Duration

@Component
@CaptureSpan(type = ITEM_META_CAPTURE_SPAN_TYPE)
class BlockchainTokenUriResolver(
    private val sender: MonoTransactionSender,
    private val tokenRepository: TokenRepository,
    @Value("\${api.properties.request-timeout}") requestTimeout: Long
) {

    private val timeout = Duration.ofMillis(requestTimeout)

    suspend fun getCollectionName(itemId: ItemId): String? {
        val token: Token = tokenRepository.findById(itemId.token).awaitFirstOrNull() ?: return null
        @Suppress("ReactiveStreamsUnusedPublisher")
        return when (token.standard) {
            TokenStandard.ERC1155 -> RaribleToken(itemId.token, sender).name()
            TokenStandard.ERC721 -> MintableToken(itemId.token, sender).name()
            else -> Mono.empty()
        }.onErrorResume {
            logMetaLoading(itemId, "failed to get name() from contract: ${it.message}", warn = true)
            Mono.empty()
        }.awaitFirstOrNull()
    }

    suspend fun getUri(itemId: ItemId): String? {
        val token = tokenRepository.findById(itemId.token).awaitFirstOrNull()
        if (token == null) {
            logMetaLoading(itemId, "token is not found", warn = true)
            return null
        }
        val result = when (token.standard) {
            TokenStandard.ERC1155 -> getErc1155TokenUri(itemId)
            TokenStandard.ERC721 -> getErc721TokenUri(itemId)
            else -> null
        }
        return if (result.isNullOrBlank()) null else result
    }

    private suspend fun getErc1155TokenUri(itemId: ItemId): String? {
        return RaribleToken(itemId.token, sender)
            .uri(itemId.tokenId.value)
            .timeout(timeout)
            .onErrorResume {
                logMetaLoading(itemId, "failed to get 'uri' from contract: ${it.message}", warn = true)
                Mono.empty()
            }.awaitFirstOrNull()
    }

    private suspend fun getErc721TokenUri(itemId: ItemId): String? {
        return MintableToken(itemId.token, sender)
            .tokenURI(itemId.tokenId.value)
            .timeout(timeout)
            .onErrorResume {
                logMetaLoading(itemId, "failed get 'tokenURI' from contract: ${it.message}", warn = true)
                Mono.empty()
            }.awaitFirstOrNull()
    }
}