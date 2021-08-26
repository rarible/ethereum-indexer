package com.rarible.protocol.nftorder.listener.service

import com.rarible.core.common.convert
import com.rarible.core.common.nowMillis
import com.rarible.core.common.optimisticLock
import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.dto.OrderDto
import com.rarible.protocol.nftorder.core.data.ItemEnrichmentData
import com.rarible.protocol.nftorder.core.data.ItemSellStats
import com.rarible.protocol.nftorder.core.event.ItemEvent
import com.rarible.protocol.nftorder.core.event.ItemEventDelete
import com.rarible.protocol.nftorder.core.event.ItemEventListener
import com.rarible.protocol.nftorder.core.event.ItemEventUpdate
import com.rarible.protocol.nftorder.core.model.Item
import com.rarible.protocol.nftorder.core.model.ItemId
import com.rarible.protocol.nftorder.core.model.OwnershipId
import com.rarible.protocol.nftorder.core.service.ItemService
import com.rarible.protocol.nftorder.core.service.OwnershipService
import com.rarible.protocol.nftorder.core.util.spent
import org.slf4j.LoggerFactory
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

@Component
class ItemEventService(
    private val conversionService: ConversionService,
    private val itemService: ItemService,
    private val ownershipService: OwnershipService,
    private val itemEventListeners: List<ItemEventListener>,
    private val bestOrderService: BestOrderService
) {

    private val logger = LoggerFactory.getLogger(ItemEventService::class.java)

    // If ownership was updated, we need to recalculate totalStock/sellers for related item
    suspend fun onOwnershipUpdated(ownershipId: OwnershipId) {
        val itemId = ItemId(ownershipId.token, ownershipId.tokenId)
        optimisticLock {
            val item = itemService.get(itemId)
            if (item == null) {
                logger.debug(
                    "Item [{}] not found in DB, skipping sell stats update on Ownership event: [{}]",
                    itemId, ownershipId
                )
            } else {
                val sellStats = ownershipService.getItemSellStats(itemId)
                if (item.sellers != sellStats.sellers && item.totalStock != sellStats.totalStock) {
                    val updatedItem = item.copy(sellers = sellStats.sellers, totalStock = sellStats.totalStock)
                    logger.info(
                        "Updating Item [{}] with new sell stats, was [{}] , now: [{}]",
                        itemId, ItemSellStats(item.sellers, item.totalStock), sellStats
                    )
                    itemService.save(updatedItem)
                    notify(ItemEventUpdate(updatedItem))
                } else {
                    logger.debug("Sell stats of Item [{}] are the same as before Ownership event [{}], skipping update")
                }
            }
        }
    }

    suspend fun onItemUpdated(nftItem: NftItemDto) {
        val rawItem = conversionService.convert<Item>(nftItem)
        val enrichmentData = itemService.getEnrichmentData(rawItem.id)
        val updated = itemService.enrichItem(rawItem, enrichmentData)
        optimisticLock {
            val existing = itemService.get(updated.id)
            if (enrichmentData.isNotEmpty()) {
                updateItem(existing, updated, enrichmentData)
            } else if (existing != null) {
                deleteItem(updated.id)
            }
        }
        notify(ItemEventUpdate(updated))
    }

    suspend fun onItemBestSellOrderUpdated(itemId: ItemId, order: OrderDto) {
        updateOrder(itemId) { item ->
            item.copy(bestSellOrder = bestOrderService.getBestSellOrder(item, order))
        }
    }

    suspend fun onItemBestBidOrderUpdated(itemId: ItemId, order: OrderDto) {
        updateOrder(itemId) { item ->
            item.copy(bestBidOrder = bestOrderService.getBestBidOrder(item, order))
        }
    }

    private suspend fun updateOrder(itemId: ItemId, orderUpdateAction: suspend (item: Item) -> Item) {
        val item = optimisticLock {
            val fetchedItem = itemService.getOrFetchItemById(itemId)
            val item = fetchedItem.entity
            val updated = orderUpdateAction(item)
            if (ItemEnrichmentData.isNotEmpty(updated)) {
                itemService.save(updated)
            } else if (!fetchedItem.isFetched) {
                deleteItem(itemId)
            }
            updated
        }
        notify(ItemEventUpdate(item))
    }

    private suspend fun updateItem(existing: Item?, updated: Item, data: ItemEnrichmentData) {
        val now = nowMillis()
        if (existing == null) {
            itemService.save(updated)
        } else {
            itemService.save(updated.copy(version = existing.version))
        }
        val operation = if (existing == null) "Inserted" else "Updated"
        logger.info(
            "{} Item [{}] with data: totalStock = {}, sellers = {}, bestSellOrder = [{}], bestBidOrder = [{}], unlockable = [{}] ({}ms)",
            operation,
            updated.id,
            data.totalStock,
            data.sellers,
            data.bestSellOrder?.hash,
            data.bestBidOrder?.hash,
            data.unlockable,
            spent(now)
        )
    }

    private suspend fun deleteItem(itemId: ItemId) {
        itemService.delete(itemId)
        logger.info("Deleted Item [{}] without enrichment data", itemId)
    }

    suspend fun onItemDeleted(itemId: ItemId) {
        val result = itemService.delete(itemId)
        if (result != null && result.deletedCount > 0) {
            logger.info("Deleted Item [{}] since it removed from NFT-Indexer", itemId)
        }
        notify(ItemEventDelete(itemId))
    }

    suspend fun onLockCreated(itemId: ItemId) {
        logger.info("Updating Item [{}] marked as Unlockable", itemId)
        val item = itemService.getOrFetchEnrichedItemById(itemId).entity.copy(unlockable = true)
        itemService.save(item)
        notify(ItemEventUpdate(item))
    }

    private suspend fun notify(event: ItemEvent) {
        itemEventListeners.forEach { it.onEvent(event) }
    }
}