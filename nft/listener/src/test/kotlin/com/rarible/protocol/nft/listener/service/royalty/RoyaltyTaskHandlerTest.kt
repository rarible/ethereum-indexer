package com.rarible.protocol.nft.listener.service.royalty

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.contracts.royalties.RoyaltiesRegistry
import com.rarible.protocol.nft.core.model.Item
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.repository.RoyaltyRepository
import com.rarible.protocol.nft.listener.integration.AbstractIntegrationTest
import com.rarible.protocol.nft.listener.integration.IntegrationTest
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scala.Tuple2
import scalether.domain.Address
import scalether.domain.AddressFactory
import scalether.transaction.MonoGasPriceProvider
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom

@IntegrationTest
class RoyaltyTaskHandlerTest : AbstractIntegrationTest() {

    @Autowired
    private lateinit var handler: RoyaltyTaskHandler

    @Autowired
    private lateinit var royaltyRepository: RoyaltyRepository

    @Test
    fun `should save royalty`() = runBlocking {
        val item = createItem()
        itemRepository.save(item).awaitFirst()

        // set royalty
        val privateKey = Numeric.toBigInt(RandomUtils.nextBytes(32))

        val userSender = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            privateKey,
            BigInteger.valueOf(8000000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )
        val royaltyContract = RoyaltiesRegistry.deployAndWait(userSender, poller).awaitFirst()
        nftIndexerProperties.royaltyRegistryAddress = royaltyContract.address().prefixed()
        royaltyContract.__RoyaltiesRegistry_init().execute().verifySuccess()

        val royalty1 = Tuple2.apply(AddressFactory.create(), BigInteger.ONE)
        royaltyContract.setRoyaltiesByTokenAndTokenId(item.token, item.tokenId.value, listOf(royalty1).toTypedArray()).execute()
            .verifySuccess()

        val address = handler.runLongTask(null, "").firstOrNull()
        assertEquals(item.token, Address.apply(address))

        // check royalty in the cache
        assertEquals(1, royaltyRepository.count().awaitFirst())
        val royaltyPE = royaltyRepository.findByItemId(item.id).awaitFirstOrNull()
        Assertions.assertNotNull(royaltyPE)
        assertEquals(royalty1._1, royaltyPE?.royalty?.get(0)?.account)
        assertEquals(royalty1._2.toInt(), royaltyPE?.royalty?.get(0)?.value)
    }

    fun createItem(): Item {
        val token = AddressFactory.create()
        val tokenId = EthUInt256.of(ThreadLocalRandom.current().nextLong(1, 10000))
        return Item(
            token = token,
            tokenId = tokenId,
            creators = listOf(createPart(), createPart()),
            supply = EthUInt256.of(ThreadLocalRandom.current().nextLong(1, 10000)),
            lazySupply = EthUInt256.of(ThreadLocalRandom.current().nextLong(1, 10000)),
            royalties = (1..ThreadLocalRandom.current().nextInt(1, 20)).map { createPart() },
            owners = (1..ThreadLocalRandom.current().nextInt(1, 20)).map { AddressFactory.create() },
            date = nowMillis(),
            isRaribleContract = false
        )
    }

    fun createPart(): Part {
        return Part(
            account = AddressFactory.create(),
            value = ThreadLocalRandom.current().nextInt(1, 10000)
        )
    }

    // restoring address after tests
    private lateinit var royaltyRegistryAddress: String

    @BeforeEach
    fun remember() {
        royaltyRegistryAddress = nftIndexerProperties.royaltyRegistryAddress
    }
    @AfterEach
    fun cleanup() {
        nftIndexerProperties.royaltyRegistryAddress = royaltyRegistryAddress
    }
}
