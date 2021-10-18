package com.rarible.protocol.nft.api.service.item

import com.rarible.core.common.convert
import com.rarible.protocol.dto.LazyNftDto
import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.dto.NftItemFilterDto
import com.rarible.protocol.dto.NftItemMetaDto
import com.rarible.protocol.dto.NftItemRoyaltyDto
import com.rarible.protocol.dto.NftItemRoyaltyListDto
import com.rarible.protocol.nft.api.domain.ItemContinuation
import com.rarible.protocol.nft.api.exceptions.EntityNotFoundApiException
import com.rarible.protocol.nft.api.service.item.ItemFilterCriteria.toCriteria
import com.rarible.protocol.nft.core.model.ExtendedItem
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.repository.history.LazyNftItemHistoryRepository
import com.rarible.protocol.nft.core.repository.item.ItemRepository
import com.rarible.protocol.nft.core.service.RoyaltyService
import com.rarible.protocol.nft.core.service.item.meta.ItemMetaService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

@Component
class ItemService(
    private val conversionService: ConversionService,
    private val itemMetaService: ItemMetaService,
    private val royaltyService: RoyaltyService,
    private val itemRepository: ItemRepository,
    private val lazyNftItemHistoryRepository: LazyNftItemHistoryRepository
) {
    suspend fun get(itemId: ItemId): NftItemDto {
        val item = itemRepository
            .findById(itemId).awaitFirstOrNull()
            ?: throw EntityNotFoundApiException("Item ", itemId)
        val meta = itemMetaService.getItemMetadata(itemId)
        return conversionService.convert(ExtendedItem(item, meta))
    }

    suspend fun getLazy(itemId: ItemId): LazyNftDto {
        return lazyNftItemHistoryRepository
            .findById(itemId).awaitFirstOrNull()
            ?.let { conversionService.convert<LazyNftDto>(it) }
            ?: throw EntityNotFoundApiException("Lazy Item", itemId)
    }

    suspend fun getMeta(itemId: ItemId): NftItemMetaDto {
        return itemMetaService
            .getItemMetadata(itemId)
            .let { conversionService.convert(it) }
    }

    suspend fun getRoyalty(itemId: ItemId): NftItemRoyaltyListDto {
        val parts = royaltyService.getRoyalty(itemId)
        return NftItemRoyaltyListDto(parts.map { NftItemRoyaltyDto(it.account, it.value) })
    }

    suspend fun resetMeta(itemId: ItemId) {
        itemMetaService.resetMetadata(itemId)
    }

    suspend fun search(
        filter: NftItemFilterDto,
        continuation: ItemContinuation?,
        size: Int?
    ): List<ExtendedItem> = coroutineScope {
        val items = itemRepository.search(filter.toCriteria(continuation, size))
        items.map { item ->
            async {
                val meta = itemMetaService.getItemMetadata(item.id)
                ExtendedItem(item, meta)
            }
        }.awaitAll()
    }
}
