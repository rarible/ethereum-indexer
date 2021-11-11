package com.rarible.protocol.nft.api.e2e.items

import com.ninjasquad.springmockk.MockkBean
import com.rarible.core.test.data.randomAddress
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.nft.validation.LazyNftValidator
import com.rarible.ethereum.nft.validation.ValidationResult
import com.rarible.protocol.contracts.erc721.rarible.ERC721Rarible
import com.rarible.protocol.contracts.erc721.rarible.user.ERC721RaribleUser
import com.rarible.protocol.dto.LazyErc1155Dto
import com.rarible.protocol.dto.LazyErc721Dto
import com.rarible.protocol.dto.LazyNftDto
import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.dto.PartDto
import com.rarible.protocol.nft.api.e2e.End2EndTest
import com.rarible.protocol.nft.api.e2e.SpringContainerBaseTest
import com.rarible.protocol.nft.api.e2e.data.createLazyErc721Dto
import com.rarible.protocol.nft.api.e2e.data.createToken
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.model.TokenFeature
import com.rarible.protocol.nft.core.repository.TokenRepository
import com.rarible.protocol.nft.core.repository.history.LazyNftItemHistoryRepository
import io.daonomic.rpc.domain.Request
import io.daonomic.rpc.domain.Word
import io.mockk.coEvery
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.web3j.crypto.Keys
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scalether.domain.Address
import scalether.domain.response.TransactionReceipt
import scalether.transaction.MonoGasPriceProvider
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import scalether.transaction.MonoTransactionSender
import java.math.BigInteger

@End2EndTest
class LazyMintControllerFt : SpringContainerBaseTest() {

    @Autowired
    private lateinit var lazyNftItemHistoryRepository: LazyNftItemHistoryRepository

    @MockkBean
    private lateinit var lazyNftValidator: LazyNftValidator

    @Autowired
    private lateinit var tokenRepository: TokenRepository

    @Autowired
    private lateinit var sender: MonoTransactionSender

    private val privateKey = Numeric.toBigInt(RandomUtils.nextBytes(32))

    private lateinit var creatorSender: MonoTransactionSender
    private lateinit var erc721: ERC721Rarible
    private lateinit var erc721User: ERC721RaribleUser

    @BeforeEach
    fun before() = runBlocking<Unit> {
        coEvery { lazyNftValidator.validate(any()) } returns ValidationResult.Valid
        creatorSender = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            privateKey,
            BigInteger.valueOf(2_000_000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )
        erc721 = ERC721Rarible.deployAndWait(creatorSender, poller).awaitSingle()
        erc721.__ERC721Rarible_init("Test", "TestSymbol", "BASE", "URI").execute().verifySuccess()

        erc721User = ERC721RaribleUser.deployAndWait(creatorSender, poller).awaitSingle()
        erc721User.__ERC721RaribleUser_init("Test", "TestSymbol", "BASE", "URI", emptyArray()).execute().verifySuccess()
    }

    @Test
    fun `should any user mints ERC721`() = runBlocking<Unit> {
        val creator = randomAddress()
        val tokenId = EthUInt256.of("0x${scalether.util.Hex.to(creator.bytes())}00000000000000000000006B")

        val lazyItemDto = createLazyErc721Dto().copy(
            contract = erc721.address(),
            tokenId = tokenId.value,
            creators = listOf(PartDto(creator, 10000)))

        val token = createToken().copy(id = lazyItemDto.contract, features = setOf(TokenFeature.MINT_AND_TRANSFER))
        tokenRepository.save(token).awaitFirst()
        val itemDto = nftLazyMintApiClient.mintNftAsset(lazyItemDto).awaitFirst()
        checkItemDto(lazyItemDto, itemDto)
    }

    @Test
    fun `should only owner mints ERC721User`() = runBlocking<Unit> {
        val creator = Address.apply(Keys.getAddressFromPrivateKey(privateKey))
        val tokenId = EthUInt256.of("0x${scalether.util.Hex.to(creator.bytes())}00000000000000000000006B")

        val lazyItemDto = createLazyErc721Dto().copy(
            contract = erc721User.address(),
            tokenId = tokenId.value,
            creators = listOf(PartDto(creator, 10000)))

        val token = createToken().copy(id = lazyItemDto.contract, features = setOf(TokenFeature.MINT_AND_TRANSFER))
        tokenRepository.save(token).awaitFirst()
        val itemDto = nftLazyMintApiClient.mintNftAsset(lazyItemDto).awaitFirst()
        checkItemDto(lazyItemDto, itemDto)
    }

    @Disabled
    @Test
    fun `shouldn't random user mints ERC721User`() = runBlocking<Unit> {
        val creator = randomAddress()
        val tokenId = EthUInt256.of("0x${scalether.util.Hex.to(creator.bytes())}00000000000000000000006B")

        val lazyItemDto = createLazyErc721Dto().copy(
            contract = erc721User.address(),
            tokenId = tokenId.value,
            creators = listOf(PartDto(creator, 10000)))

        val token = createToken().copy(id = lazyItemDto.contract, features = setOf(TokenFeature.MINT_AND_TRANSFER))
        tokenRepository.save(token).awaitFirst()
        assertThatThrownBy {
            runBlocking { nftLazyMintApiClient.mintNftAsset(lazyItemDto).awaitFirst() }
        }
    }

    @Test
    fun `should get bad request if token id not start with first creator address`() = runBlocking<Unit> {
        val lazyNftDto = createLazyErc721Dto()
        val token = createToken().copy(id = lazyNftDto.contract, features = setOf(TokenFeature.MINT_AND_TRANSFER))

        tokenRepository.save(token).awaitFirst()

        assertThatThrownBy {
            runBlocking { nftLazyMintApiClient.mintNftAsset(lazyNftDto).awaitFirst() }
        }
    }

    private suspend fun checkItemDto(lazyItemDto: LazyNftDto, itemDto: NftItemDto) {
        val itemId = ItemId(lazyItemDto.contract, EthUInt256(lazyItemDto.tokenId))
        assertThat(itemDto.id).isEqualTo(itemId.decimalStringValue)
        assertThat(itemDto.contract).isEqualTo(lazyItemDto.contract)
        assertThat(itemDto.tokenId).isEqualTo(lazyItemDto.tokenId)

        when (lazyItemDto) {
            is LazyErc1155Dto -> {
                assertThat(itemDto.supply).isEqualTo(lazyItemDto.supply)
                assertThat(itemDto.lazySupply).isEqualTo(lazyItemDto.supply)
            }
            is LazyErc721Dto -> {
                assertThat(itemDto.supply).isEqualTo(EthUInt256.ONE.value)
                assertThat(itemDto.lazySupply).isEqualTo(EthUInt256.ONE.value)
            }
        }

        assertThat(itemDto.owners).isEqualTo(listOf(lazyItemDto.creators.first().account))

        assertThat(itemDto.royalties.size).isEqualTo(lazyItemDto.royalties.size)

        itemDto.royalties.forEachIndexed { index, royaltyDto ->
            assertThat(royaltyDto.account).isEqualTo(lazyItemDto.royalties[index].account)
            assertThat(royaltyDto.value).isEqualTo(lazyItemDto.royalties[index].value)
        }

        assertThat(itemDto.creators).hasSize(lazyItemDto.creators.size)

        itemDto.creators.forEachIndexed { index, creatorDto ->
            assertThat(creatorDto.account).isEqualTo(lazyItemDto.creators[index].account)
            assertThat(creatorDto.value).isEqualTo(lazyItemDto.creators[index].value)
        }

        val lazyMint = lazyNftItemHistoryRepository.findLazyMintById(itemId).awaitFirst()
        assertThat(lazyMint.token).isEqualTo(lazyItemDto.contract)
        assertThat(lazyMint.tokenId.value).isEqualTo(lazyItemDto.tokenId)

        lazyMint.creators.forEachIndexed { index, it ->
            assertThat(it)
                .hasFieldOrPropertyWithValue(Part::account.name, lazyItemDto.creators[index].account)
                .hasFieldOrPropertyWithValue(Part::value.name, lazyItemDto.creators[index].value)
        }

        lazyMint.royalties.forEachIndexed { index, royalty ->
            assertThat(royalty)
                .hasFieldOrPropertyWithValue(Part::account.name, lazyItemDto.royalties[index].account)
                .hasFieldOrPropertyWithValue(Part::value.name, lazyItemDto.royalties[index].value)
        }
    }

    protected suspend fun Mono<Word>.verifySuccess(): TransactionReceipt {
        val receipt = waitReceipt()
        Assertions.assertTrue(receipt.success()) {
            val result = ethereum.executeRaw(
                Request(1, "trace_replayTransaction", scalether.java.Lists.toScala(
                    receipt.transactionHash().toString(),
                    scalether.java.Lists.toScala("trace", "stateDiff")
                ), "2.0")
            ).block()!!
            "traces: ${result.result().get()}"
        }
        return receipt
    }

    private suspend fun Mono<Word>.waitReceipt(): TransactionReceipt {
        val value = this.awaitFirstOrNull()
        require(value != null) { "txHash is null" }
        return ethereum.ethGetTransactionReceipt(value).awaitFirst().get()
    }
}
