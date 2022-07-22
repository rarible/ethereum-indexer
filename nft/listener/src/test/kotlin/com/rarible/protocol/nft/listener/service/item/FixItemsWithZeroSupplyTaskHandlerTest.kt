package com.rarible.protocol.nft.listener.service.item

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.model.Item
import com.rarible.protocol.nft.core.model.Token
import com.rarible.protocol.nft.core.model.TokenStandard
import com.rarible.protocol.nft.listener.integration.AbstractIntegrationTest
import com.rarible.protocol.nft.listener.integration.IntegrationTest
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import scalether.domain.AddressFactory
import java.util.UUID

private const val ZERO_SUPPLY_ITEM_AMOUNT = 10
private const val NON_ZERO_SUPPLY_ITEM_AMOUNT = 15

@IntegrationTest
@FlowPreview
class FixItemsWithZeroSupplyTaskHandlerTest  : AbstractIntegrationTest(){
    @Autowired
    private lateinit var fixItemsWithZeroSupplyTaskHandler: FixItemsWithZeroSupplyTaskHandler

    @BeforeEach
    fun setup() = runBlocking<Unit> {
        nftItemHistoryRepository.createIndexes()
    }

    @Test
    fun `should set deleted to true for all items with zero supply`() = runBlocking<Unit> {

        val zeroSupplyItemsList = putItemToDataBase(ZERO_SUPPLY_ITEM_AMOUNT, true)
        val nonZeroSupplyItemsList = putItemToDataBase(NON_ZERO_SUPPLY_ITEM_AMOUNT, false)

        val updateItemsAmount = fixItemsWithZeroSupplyTaskHandler.runLongTask(null, "").count()

        assertThat(updateItemsAmount).isEqualTo(zeroSupplyItemsList.size)

        zeroSupplyItemsList.forEach {
            val updatedItem = itemRepository.findById(ItemId(it.token, it.tokenId)).awaitFirstOrNull()
            assertThat(updatedItem?.deleted).isTrue()
        }

        nonZeroSupplyItemsList.forEach {
            val updatedItem = itemRepository.findById(ItemId(it.token, it.tokenId)).awaitFirstOrNull()
            assertThat(updatedItem?.deleted).isFalse()
        }

    }

    fun createToken(): Token {
        return Token(
            id = AddressFactory.create(),
            name = UUID.randomUUID().toString(),
            standard = TokenStandard.values().random(),
        )
    }

    fun createItem(): Item {
        val token = AddressFactory.create()
        val tokenId = EthUInt256.of(0)
        return Item(
            token = token,
            tokenId = tokenId,
            supply = EthUInt256.of(0),
            royalties = listOf(Part.fullPart(AddressFactory.create())),
            date = nowMillis(),
            isRaribleContract = false
        )
    }

    private suspend fun putItemToDataBase(size: Int, isSupplyZero: Boolean): List<Item> {
        val result = mutableListOf<Item>()

        repeat(size) {
            val token = createToken()
            tokenRepository.save(token).awaitFirst()

            var supply = EthUInt256.ONE
            if (isSupplyZero) {
                supply = EthUInt256.ZERO
            }

            val item =
                createItem().copy(
                    token = token.id,
                    tokenId = EthUInt256.ZERO,
                    deleted = false,
                    supply = supply
                )

            itemRepository.save(item).awaitFirst()

            val log = ItemReduceTaskHandlerTest.createMintLog(
                blockNumber = 1,
                token = token.id,
                tokenId = EthUInt256.ZERO,
                value = supply
            )

            nftItemHistoryRepository.save(log).awaitFirst()
            result.add(item)
        }

        return result
    }
}