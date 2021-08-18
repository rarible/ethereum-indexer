package com.rarible.protocol.nftorder.listener.handler

import com.rarible.core.test.data.randomAddress
import com.rarible.core.test.data.randomString
import com.rarible.core.test.data.randomWord
import com.rarible.core.test.wait.Wait
import com.rarible.protocol.dto.OrderDto
import com.rarible.protocol.dto.OrderUpdateEventDto
import com.rarible.protocol.dto.PlatformDto
import com.rarible.protocol.nftorder.core.service.ItemService
import com.rarible.protocol.nftorder.core.service.OwnershipService
import com.rarible.protocol.nftorder.listener.test.AbstractIntegrationTest
import com.rarible.protocol.nftorder.listener.test.IntegrationTest
import com.rarible.protocol.nftorder.listener.test.data.*
import io.daonomic.rpc.domain.Word
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class OrderEventHandlerIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var orderEventHandler: OrderEventHandler

    @Autowired
    lateinit var itemService: ItemService

    @Autowired
    lateinit var ownershipService: OwnershipService

    /**
     * The largest test case - received order contains both make/take assetTypes with related items.
     * Both of these items are not present in nft-order Mongo and should be fetched from outside.
     * Ownership, related to make-item should also be fetched outside.
     */
    @Test
    fun `make, take items and ownership fetched and created`() = runWithKafka {
        // Some random data, make-Item linked with Ownership
        val makeItemId = randomItemId()
        val takeItemId = randomItemId()
        val ownershipId = randomOwnershipId(makeItemId)

        val nftMakeItemDto = randomNftItemDto(makeItemId, randomPartDto(ownershipId.owner))
        val nftTakeItemDto = randomNftItemDto(takeItemId)
        val nftOwnership = randomNftOwnershipDto(nftMakeItemDto)

        val bestSell = randomLegacyOrderDto(makeItemId)
        val bestBid = randomLegacyOrderDto(takeItemId)
        val bestSellByOwnership = randomLegacyOrderDto(makeItemId, ownershipId.owner)

        // Since we don't have any data in Mongo, we need to fetch entities and enrichment data via HTTP API
        nftItemControllerApiMock.mockGetNftItemById(makeItemId, nftMakeItemDto)
        nftItemControllerApiMock.mockGetNftItemById(takeItemId, nftTakeItemDto)
        nftOwnershipControllerApiMock.mockGetNftOwnershipById(ownershipId, nftOwnership)

        // Unlockable responses mocks
        lockControllerApiMock.mockIsUnlockable(makeItemId, true)
        lockControllerApiMock.mockIsUnlockable(takeItemId, false)

        // For make-item there is no bestBid Order, and for take-item there is no bestSell Order
        orderControllerApiMock.mockGetSellOrdersByItem(makeItemId, bestSell)
        orderControllerApiMock.mockGetBidOrdersByItem(makeItemId)
        orderControllerApiMock.mockGetSellOrdersByItem(takeItemId)
        orderControllerApiMock.mockGetBidOrdersByItem(takeItemId, bestBid)
        orderControllerApiMock.mockGetSellOrdersByItem(ownershipId, bestSellByOwnership)

        val updatedOrder =
            randomLegacyOrderDto(randomAssetErc721(makeItemId), ownershipId.owner, randomAssetErc1155(takeItemId))

        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Ensure all three entities are stored in Mongo and has fields equivalent to DTO's fields
        val makeItem = itemService.get(makeItemId)!!
        assertItemAndDtoEquals(makeItem, nftMakeItemDto)
        assertThat(makeItem.unlockable).isTrue()
        assertThat(makeItem.bestSellOrder).isEqualTo(bestSell)
        assertThat(makeItem.bestBidOrder).isNull()

        val takeItem = itemService.get(takeItemId)!!
        assertItemAndDtoEquals(takeItem, nftTakeItemDto)
        assertThat(takeItem.unlockable).isFalse()
        assertThat(takeItem.bestSellOrder).isNull()
        assertThat(takeItem.bestBidOrder).isEqualTo(bestBid)

        val ownership = ownershipService.get(ownershipId)!!
        assertOwnershipAndNftDtoEquals(ownership, nftOwnership)
        assertThat(ownership.bestSellOrder).isEqualTo(bestSellByOwnership)

        Wait.waitAssert {
            assertThat(itemEvents).hasSize(3)
            assertThat(ownershipEvents).hasSize(1)
        }
    }

    @Test
    fun `make item and ownership updated`() = runWithKafka {
        // Starting with item/ownership already saved in Mongo, both of them have no enrich data
        val item = randomItem(randomPart())
        val makeItemId = item.id
        val ownership = randomOwnership(item)
        val ownershipId = ownership.id
        ownershipService.save(ownership)
        itemService.save(item)

        // If ownership and item already stored in Mongo, no HTTP calls should be executed,
        // updated order should be taken from event
        val bestSell = randomLegacyOrderDto(makeItemId, ownershipId.owner)
        orderEventHandler.handle(createOrderUpdateEvent(bestSell))

        // Ensure existing entities updated with enrich data
        val updatedMakeItem = itemService.get(makeItemId)!!
        assertThat(updatedMakeItem.bestSellOrder).isEqualTo(bestSell)

        // Ensure existing entity updated order from event
        verify(exactly = 0) { orderControllerApi.getSellOrdersByItem(any(), any(), any(), any(), any(), any(), any()) }

        val updateOwnership = ownershipService.get(ownershipId)!!
        assertThat(updateOwnership.bestSellOrder).isEqualTo(bestSell)
        assertThat(updatedMakeItem.sellers).isEqualTo(1)
        assertThat(updatedMakeItem.totalStock).isEqualTo(bestSell.makeStock)
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(2)
            assertThat(ownershipEvents).hasSize(1)
        }
    }

    @Test
    fun `take item updated`() = runWithKafka {
        // Starting with item already saved in Mongo without enrich data
        val item = randomItem(randomPart())
        val takeItemId = item.id
        itemService.save(item)

        // If item already stored in Mongo, no HTTP calls should be executed,
        // updated order should be taken from event
        val updatedOrder = randomLegacyOrderDto(randomAssetErc20(), randomAddress(), randomAssetErc1155(takeItemId))
        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Ensure existing entity updated order from event
        verify(exactly = 0) { orderControllerApi.getOrderBidsByItem(any(), any(), any(), any(), any(), any(), any()) }

        val updatedMakeItem = itemService.get(takeItemId)!!
        assertThat(updatedMakeItem.bestBidOrder).isEqualTo(updatedOrder)
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(1)
        }
    }

    @Test
    fun `take item updated - OpenSea order`() = runWithKafka {
        // Starting with item already saved in Mongo with cancelled OpenSea order
        val itemId = randomItemId()
        val current = randomOpenSeaV1OrderDto(randomAssetErc20(), randomAddress(), randomAssetErc1155(itemId))
        val item = randomItem(itemId, randomPart()).copy(bestBidOrder = current)
        itemService.save(item)

        // Fetching best bid order, which is OpenSeaOrder - and there is no Rarible best order
        val openSeaBestBid = randomOpenSeaV1OrderDto(itemId)
        orderControllerApiMock.mockGetBidOrdersByItem(itemId, PlatformDto.ALL, openSeaBestBid)
        orderControllerApiMock.mockGetBidOrdersByItem(itemId, PlatformDto.RARIBLE)

        val updatedOrder = current.copy(hash = current.hash, cancelled = true)
        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Ensure item updated with OpenSea order
        val updatedMakeItem = itemService.get(itemId)!!
        assertThat(updatedMakeItem.bestBidOrder).isEqualTo(openSeaBestBid)

        verify(exactly = 1) {
            orderControllerApi.getOrderBidsByItem(any(), any(), any(), any(), eq(PlatformDto.ALL), any(), any())
        }
        verify(exactly = 1) {
            orderControllerApi.getOrderBidsByItem(any(), any(), any(), any(), eq(PlatformDto.RARIBLE), any(), any())
        }
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(1)
        }
    }

    @Test
    fun `take item updated - preferred Rarible Order`() = runWithKafka {
        // Starting with item already saved in Mongo with cancelled order
        val itemId = randomItemId()
        val current = randomOpenSeaV1OrderDto(randomAssetErc20(), randomAddress(), randomAssetErc1155(itemId))
        val item = randomItem(itemId, randomPart()).copy(bestBidOrder = current)
        itemService.save(item)

        // Fetching best bid order, which is OpenSeaOrder, but we also have Rarible bestBid
        val openSeaBestBid = randomOpenSeaV1OrderDto(itemId)
        val raribleBestBid = randomLegacyOrderDto(itemId)
        orderControllerApiMock.mockGetBidOrdersByItem(itemId, PlatformDto.ALL, openSeaBestBid)
        orderControllerApiMock.mockGetBidOrdersByItem(itemId, PlatformDto.RARIBLE, raribleBestBid)

        val updatedOrder = current.copy(cancelled = true)
        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Ensure item updated with Rarible order
        val updatedMakeItem = itemService.get(itemId)!!
        assertThat(updatedMakeItem.bestBidOrder).isEqualTo(raribleBestBid)

        verify(exactly = 1) {
            orderControllerApi.getOrderBidsByItem(any(), any(), any(), any(), eq(PlatformDto.ALL), any(), any())
        }
        verify(exactly = 1) {
            orderControllerApi.getOrderBidsByItem(any(), any(), any(), any(), eq(PlatformDto.RARIBLE), any(), any())
        }
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(1)
        }
    }

    @Test
    fun `make item and ownership deleted, no enrich data`() = runWithKafka {
        // Starting with item/ownership already saved in Mongo, both of them have bestSellOrder
        val itemId = randomItemId()
        val orderHash = Word.apply(randomWord())
        val item =
            randomItem(itemId, randomPart()).copy(bestSellOrder = randomLegacyOrderDto(itemId).copy(hash = orderHash))
        val ownership = randomOwnership(item).copy(bestSellOrder = randomLegacyOrderDto(itemId).copy(hash = orderHash))
        ownershipService.save(ownership)
        itemService.save(item)

        // HTTP API returns NULL bestSellOrder
        orderControllerApiMock.mockGetSellOrdersByItem(item.id)
        orderControllerApiMock.mockGetSellOrdersByItem(ownership.id)

        // Updated Order should be cancelled - in such case we will try to fetch actual best Order
        val updatedOrder = randomLegacyOrderDto(item.id, ownership.id.owner).copy(hash = orderHash, cancelled = true)

        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Both entities should be deleted
        assertThat(itemService.get(item.id)).isNull()
        assertThat(ownershipService.get(ownership.id)).isNull()
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(1)
            assertThat(ownershipEvents).hasSize(1)
        }
    }

    @Test
    fun `take item NOT deleted if some enrich data remains`() = runWithKafka {
        // Unlockable set to TRUE, so even we don't have bestBidOrder, we have to keep this entity in Mongo
        val itemId = randomItemId()
        val current = randomLegacyOrderDto(randomAssetErc20(), randomAddress(), randomAssetErc1155(itemId))
        val item = randomItem(itemId, randomPart()).copy(unlockable = true, bestBidOrder = current)
        itemService.save(item)

        // HTTP API returns NULL best bid order
        orderControllerApiMock.mockGetBidOrdersByItem(itemId)

        val updatedOrder = current.copy(cancelled = true)
        orderEventHandler.handle(createOrderUpdateEvent(updatedOrder))

        // Unlockable is still TRUE, bestBid Order reset
        val update = itemService.get(item.id)!!
        assertThat(update.bestBidOrder).isNull()
        assertThat(update.bestSellOrder).isNull()
        assertThat(update.unlockable).isTrue()
        Wait.waitAssert {
            assertThat(itemEvents).hasSize(1)
        }
    }

    private fun createOrderUpdateEvent(order: OrderDto): OrderUpdateEventDto {
        return OrderUpdateEventDto(
            randomString(),
            order.hash.hex(),
            order
        )
    }

}