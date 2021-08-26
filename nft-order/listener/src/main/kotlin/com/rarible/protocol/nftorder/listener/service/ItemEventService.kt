package com.rarible.protocol.nftorder.listener.service

import com.rarible.core.common.convert
import com.rarible.core.common.nowMillis
import com.rarible.core.common.optimisticLock
import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.dto.OrderDto
import com.rarible.protocol.nftorder.core.data.EnrichmentDataVerifier
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
                val refreshedSellStats = ownershipService.getItemSellStats(itemId)
                val currentSellStats = ItemSellStats(item.sellers, item.totalStock)
                if (refreshedSellStats != currentSellStats) {
                    val updatedItem =
                        item.copy(sellers = refreshedSellStats.sellers, totalStock = refreshedSellStats.totalStock)
                    logger.info(
                        "Updating Item [{}] with new sell stats, was [{}] , now: [{}]",
                        itemId, currentSellStats, refreshedSellStats
                    )
                    itemService.save(updatedItem)
                    notify(ItemEventUpdate(updatedItem))
                } else {
                    logger.debug(
                        "Sell stats of Item [{}] are the same as before Ownership event [{}], skipping update",
                        itemId, ownershipId
                    )
                }
            }
        }
    }

    suspend fun onItemUpdated(nftItem: NftItemDto) {
        val received = conversionService.convert<Item>(nftItem)
        optimisticLock {
            val existing = itemService.get(received.id)
            if (existing != null) {
                // If we have Item in DB, it also means we have some enrichment data here -
                // so we're just replacing root data and keep enrich data the same
                val updated = received.copy(
                    bestBidOrder = existing.bestBidOrder,
                    bestSellOrder = existing.bestSellOrder,
                    totalStock = existing.totalStock,
                    sellers = existing.sellers,
                    unlockable = existing.unlockable
                )
                val saved = updateItem(existing, updated)
                notify(ItemEventUpdate(saved))
            } else {
                // Otherwise, we just proxy original event
                notify(ItemEventUpdate(received))
            }
        }
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
            if (EnrichmentDataVerifier.isItemNotEmpty(updated)) {
                itemService.save(updated)
            } else if (!fetchedItem.isFetched) {
                itemService.delete(itemId)
                logger.info("Deleted Item [{}] without enrichment data", itemId)
            }
            updated
        }
        notify(ItemEventUpdate(item))
    }

    private suspend fun updateItem(existing: Item, updated: Item): Item {
        val now = nowMillis()
        val result = itemService.save(updated.copy(version = existing.version))
        logger.info(
            "Updated Item [{}] with data: totalStock = {}, sellers = {}, bestSellOrder = [{}], bestBidOrder = [{}], unlockable = [{}] ({}ms)",
            updated.id,
            updated.totalStock,
            updated.sellers,
            updated.bestSellOrder?.hash,
            updated.bestBidOrder?.hash,
            updated.unlockable,
            spent(now)
        )
        return result
    }

    suspend fun onItemDeleted(itemId: ItemId) {
        val deleted = deleteItem(itemId)
        notify(ItemEventDelete(itemId))
        if (deleted) {
            logger.info("Item [{}] deleted (removed from NFT-Indexer)", itemId)
        }
    }

    private suspend fun deleteItem(itemId: ItemId): Boolean {
        val result = itemService.delete(itemId)
        return result != null && result.deletedCount > 0
    }

    suspend fun onLockCreated(itemId: ItemId) {
        logger.info("Updating Item [{}] marked as Unlockable", itemId)
        val item = itemService.getOrFetchItemById(itemId).entity.copy(unlockable = true)
        itemService.save(item)
        notify(ItemEventUpdate(item))
    }

    private suspend fun notify(event: ItemEvent) {
        itemEventListeners.forEach { it.onEvent(event) }
    }
}