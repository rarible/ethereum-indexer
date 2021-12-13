package com.rarible.protocol.nft.api.service.item

import com.rarible.core.apm.CaptureSpan
import com.rarible.core.apm.SpanType
import com.rarible.core.cache.CacheService
import com.rarible.core.cache.get
import com.rarible.core.common.convert
import com.rarible.protocol.dto.LazyNftDto
import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.dto.NftItemMetaDto
import com.rarible.protocol.dto.NftItemRoyaltyDto
import com.rarible.protocol.dto.NftItemRoyaltyListDto
import com.rarible.protocol.nft.api.domain.ItemContinuation
import com.rarible.protocol.nft.api.exceptions.EntityNotFoundApiException
import com.rarible.protocol.nft.api.service.descriptor.RoyaltyCacheDescriptor
import com.rarible.protocol.nft.api.service.item.ItemFilterCriteria.toCriteria
import com.rarible.protocol.nft.core.model.ExtendedItem
import com.rarible.protocol.nft.core.model.ItemFilter
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.page.PageSize
import com.rarible.protocol.nft.core.repository.history.LazyNftItemHistoryRepository
import com.rarible.protocol.nft.core.repository.item.ItemRepository
import com.rarible.protocol.nft.core.service.item.meta.ItemMetaService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

@Component
@CaptureSpan(SpanType.APP)
class ItemService(
    private val conversionService: ConversionService,
    private val itemMetaService: ItemMetaService,
    private val royaltyCacheDescriptor: RoyaltyCacheDescriptor,
    private val cacheService: CacheService,
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
            .findLazyMintById(itemId).awaitFirstOrNull()
            ?.let { conversionService.convert<LazyNftDto>(it) }
            ?: throw EntityNotFoundApiException("Lazy Item", itemId)
    }

    suspend fun getMeta(itemId: ItemId): NftItemMetaDto {
        return itemMetaService
            .getItemMetadata(itemId)
            .let { conversionService.convert(it) }
    }

    suspend fun getRoyalty(itemId: ItemId): NftItemRoyaltyListDto = coroutineScope {
        val parts = cacheService
            .get(itemId.toString(), royaltyCacheDescriptor, true)
            .awaitSingle()
        NftItemRoyaltyListDto(parts.map { NftItemRoyaltyDto(it.account, it.value) })
    }

    suspend fun resetMeta(itemId: ItemId) {
        itemMetaService.resetMetadata(itemId)
    }

    suspend fun search(
        filter: ItemFilter,
        continuation: ItemContinuation?,
        size: Int?
    ): List<ExtendedItem> = coroutineScope {
        val requestSize = PageSize.ITEM.limit(size)
        val items = itemRepository.search(filter.toCriteria(continuation, requestSize))
        items.map { item ->
            async {
                val meta = itemMetaService.getItemMetadata(item.id)
                ExtendedItem(item, meta)
            }
        }.awaitAll()
    }
}
