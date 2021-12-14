package com.rarible.protocol.nft.core.service.composit

import com.rarible.core.entity.reducer.service.EntityService
import com.rarible.protocol.nft.core.model.CompositeEntity
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.service.item.reduce.ItemUpdateService
import com.rarible.protocol.nft.core.service.ownership.reduce.OwnershipUpdateService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.springframework.stereotype.Component

@Component
class CompositeUpdateService(
    private val itemUpdateService: ItemUpdateService,
    private val ownershipUpdateService: OwnershipUpdateService
) : EntityService<ItemId, CompositeEntity> {

    override suspend fun get(id: ItemId): CompositeEntity? {
        throw UnsupportedOperationException("Must not be called")
    }

    override suspend fun update(entity: CompositeEntity): CompositeEntity {
        return coroutineScope {
            val savedItem = entity.item?.let {
                async {
                    itemUpdateService.update(it)
                }
            }
            val savedOwnerships = entity.ownerships.map {
                async {
                    ownershipUpdateService.update(it)
                }
            }
            CompositeEntity(entity.id, savedItem?.await(), savedOwnerships.awaitAll())
        }
    }
}