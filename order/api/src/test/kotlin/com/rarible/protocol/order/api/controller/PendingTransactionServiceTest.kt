package com.rarible.protocol.order.api.controller

import com.rarible.contracts.test.erc1155.TestERC1155
import com.rarible.core.common.nowMillis
import com.rarible.ethereum.common.NewKeys
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.listener.log.domain.LogEventStatus
import com.rarible.ethereum.sign.domain.EIP712Domain
import com.rarible.protocol.contracts.common.TransferProxy
import com.rarible.protocol.contracts.common.deprecated.TransferProxyForDeprecated
import com.rarible.protocol.contracts.erc20.proxy.ERC20TransferProxy
import com.rarible.protocol.contracts.exchange.v1.ExchangeV1
import com.rarible.protocol.contracts.exchange.v1.state.ExchangeStateV1
import com.rarible.protocol.contracts.exchange.v2.ExchangeV2
import com.rarible.protocol.contracts.royalties.TestRoyaltiesProvider
import com.rarible.protocol.dto.CreateTransactionRequestDto
import com.rarible.protocol.dto.LogEventDto
import com.rarible.protocol.order.api.data.sign
import com.rarible.protocol.order.api.integration.AbstractIntegrationTest
import com.rarible.protocol.order.api.integration.IntegrationTest
import com.rarible.protocol.order.api.misc.setField
import com.rarible.protocol.order.api.service.pending.PendingTransactionService
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.service.asset.AssetTypeService
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.web3j.crypto.Keys
import org.web3j.crypto.Sign
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scala.Tuple2
import scala.Tuple3
import scala.Tuple4
import scala.Tuple9
import scalether.domain.Address
import scalether.domain.AddressFactory
import scalether.domain.response.Transaction
import scalether.domain.response.TransactionReceipt
import scalether.transaction.MonoGasPriceProvider
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import java.math.BigInteger

@IntegrationTest
@Import(PendingTransactionServiceTest.TestAssetTypeServiceConfiguration::class)
class PendingTransactionServiceTest : AbstractIntegrationTest() {

    @Autowired
    lateinit var pendingTransactionService: PendingTransactionService

    @Autowired
    lateinit var assetTypeService: AssetTypeService

    @Nested
    inner class ForExchangeV1 {
        @Test
        fun `should create pending transaction for cancel - v1`() = runBlocking<Unit> {
            val salt = BigInteger.TEN
            val tokenId = BigInteger.ONE
            val buyTokenId = BigInteger.TEN

            val userSender = newSender().second

            val beneficiary = Address.THREE()

            val buyerFeeSignerPrivateKey = Numeric.toBigInt(RandomUtils.nextBytes(32))

            val buyerFeeSigner = MonoSigningTransactionSender(
                ethereum,
                MonoSimpleNonceProvider(ethereum),
                buyerFeeSignerPrivateKey,
                BigInteger.valueOf(8000000),
                MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
            )

            val adminSender = newSender().second
            val token = TestERC1155.deployAndWait(adminSender, poller, "ipfs:/").awaitFirst()
            val buyToken = TestERC1155.deployAndWait(adminSender, poller, "ipfs:/").awaitFirst()

            val state = ExchangeStateV1.deployAndWait(userSender, poller).block()!!
            val proxy = TransferProxy.deployAndWait(userSender, poller).block()!!
            val proxyForDeprecated = TransferProxyForDeprecated.deployAndWait(userSender, poller).block()!!
            val erc20Proxy = ERC20TransferProxy.deployAndWait(userSender, poller).block()!!
            val sale = ExchangeV1.deployAndWait(userSender, poller, proxy.address(), proxyForDeprecated.address(), erc20Proxy.address(), state.address(), Address.ZERO(), beneficiary, buyerFeeSigner.from()).block()!!

            state.addOperator(sale.address())
                .execute().verifySuccess()
            proxy.addOperator(sale.address())
                .execute().verifySuccess()
            proxyForDeprecated.addOperator(sale.address())
                .execute().verifySuccess()
            erc20Proxy.addOperator(sale.address())
                .execute().verifySuccess()
            token.setApprovalForAll(proxy.address(), true)
                .execute().verifySuccess()

            val orderKey = Tuple4(
                userSender.from(),
                salt,
                Tuple3(token.address(), tokenId, LegacyAssetTypeClass.ERC1155.value),
                Tuple3(buyToken.address(), buyTokenId, LegacyAssetTypeClass.ERC1155.value)
            )

            val receipt = sale.cancel(orderKey).withSender(userSender).execute().verifySuccess()

            val makeAssetType = Erc1155AssetType(
                token.address(),
                EthUInt256.of(tokenId)
            )
            val takeAssetType = Erc1155AssetType(
                buyToken.address(),
                EthUInt256.of(buyTokenId)
            )
            val orderVersion = OrderVersion(
                maker = userSender.from(),
                taker = AddressFactory.create(),
                make = Asset(
                    type = makeAssetType,
                    value = EthUInt256.TEN
                ),
                take = Asset(
                    type = takeAssetType,
                    value = EthUInt256.TEN
                ),
                type = OrderType.RARIBLE_V1,
                salt = EthUInt256.of(salt),
                data = OrderDataLegacy(0),
                start = null,
                end = null,
                signature = null,
                createdAt = nowMillis(),
                makePriceUsd = null,
                takePriceUsd = null,
                makePrice = null,
                takePrice = null,
                makeUsd = null,
                takeUsd = null
            )

            setField(pendingTransactionService, "exchangeContracts", hashSetOf(sale.address()))

            orderUpdateService.save(orderVersion)

            coEvery { assetTypeService.toAssetType(eq(makeAssetType.token), eq(makeAssetType.tokenId)) } returns makeAssetType
            coEvery { assetTypeService.toAssetType(eq(takeAssetType.token), eq(takeAssetType.tokenId)) } returns takeAssetType

            processTransaction(receipt)

            val history = exchangeHistoryRepository.findLogEvents(orderVersion.hash, null).collectList().awaitFirst()
            assertThat(history).hasSize(1)
            assertThat(history.single().status).isEqualTo(LogEventStatus.PENDING)

            val savedOrder = orderRepository.findById(orderVersion.hash)
            assertThat(savedOrder?.pending).hasSize(1)
            assertThat(savedOrder?.pending?.single()).isInstanceOf(OrderCancel::class.java)
        }
    }

    @Nested
    inner class ForExchangeV2 {
        lateinit var token: TestERC1155
        lateinit var buyToken: TestERC1155
        lateinit var exchangeV2Contract: ExchangeV2
        lateinit var eip712Domain: EIP712Domain
        lateinit var transferProxy: TransferProxy

        @BeforeEach
        fun before() = runBlocking {
            val adminSender = newSender().second

            token = TestERC1155.deployAndWait(adminSender, poller, "ipfs:/").awaitFirst()
            buyToken = TestERC1155.deployAndWait(adminSender, poller, "ipfs:/").awaitFirst()

            transferProxy = TransferProxy.deployAndWait(adminSender, poller).awaitSingle()
            val royaltiesProvider = TestRoyaltiesProvider.deployAndWait(adminSender, poller).awaitSingle()
            exchangeV2Contract = ExchangeV2.deployAndWait(adminSender, poller).awaitSingle()
            exchangeV2Contract.__ExchangeV2_init(
                transferProxy.address(),
                Address.ZERO(),
                BigInteger.ZERO,
                Address.ZERO(),
                royaltiesProvider.address()
            ).execute().verifySuccess()

            transferProxy.addOperator(exchangeV2Contract.address()).execute().verifySuccess()

            eip712Domain = EIP712Domain(
                name = "Exchange",
                version = "2",
                chainId = BigInteger.valueOf(17),
                verifyingContract = exchangeV2Contract.address()
            )
        }

        @Test
        fun `should create pending transaction for cancel - v2`() = runBlocking<Unit> {
            val (maker, makerSender) = newSender()

            val salt = BigInteger.TEN
            val tokenId = BigInteger.ONE
            val buyTokenId = BigInteger.TEN

            val make = Asset(type = Erc1155AssetType(token.address(), EthUInt256.of(tokenId)), value = EthUInt256.TEN)
            val take = Asset(type = Erc1155AssetType(buyToken.address(), EthUInt256.of(buyTokenId)), value = EthUInt256.TEN)
            val taker = AddressFactory.create()
            val data = OrderRaribleV2DataV1(emptyList(), emptyList())

            val orderTuple = Tuple9(
                maker,
                Tuple2(make.type.forTx(), make.value.value),
                taker,
                Tuple2(take.type.forTx(), take.value.value),
                salt,
                BigInteger.ZERO,
                BigInteger.ZERO,
                data.getDataVersion(),
                data.toEthereum().bytes()
            )

            val receipt = exchangeV2Contract.cancel(orderTuple).withSender(makerSender).execute().verifySuccess()

            val orderVersion = OrderVersion(
                maker = maker,
                taker = taker,
                make = make,
                take = take,
                type = OrderType.RARIBLE_V2,
                salt = EthUInt256.of(salt),
                data = data,
                start = null,
                end = null,
                signature = null,
                createdAt = nowMillis(),
                makePriceUsd = null,
                takePriceUsd = null,
                makePrice = null,
                takePrice = null,
                makeUsd = null,
                takeUsd = null
            )
            setField(pendingTransactionService, "exchangeContracts", hashSetOf(exchangeV2Contract.address()))

            orderUpdateService.save(orderVersion)

            processTransaction(receipt)

            val history = exchangeHistoryRepository.findLogEvents(orderVersion.hash, null).collectList().awaitFirst()
            assertThat(history).hasSize(1)
            assertThat(history.single().status).isEqualTo(LogEventStatus.PENDING)

            val savedOrder = orderRepository.findById(orderVersion.hash)
            assertThat(savedOrder?.pending).hasSize(1)
            assertThat(savedOrder?.pending?.single()).isInstanceOf(OrderCancel::class.java)
        }

        @Test
        fun `should create pending transaction for matchOrders - v2`() = runBlocking<Unit> {
            val (maker, makerSender, makePrivateKey) = newSender()
            val (taker, takerSender, takePrivateKey) = newSender()

            val salt = BigInteger.TEN
            val tokenId = BigInteger.ONE
            val buyTokenId = BigInteger.TEN

            val make = Asset(type = Erc1155AssetType(token.address(), EthUInt256.of(tokenId)), value = EthUInt256.TEN)
            val take = Asset(type = Erc1155AssetType(buyToken.address(), EthUInt256.of(buyTokenId)), value = EthUInt256.TEN)

            token.mint(maker, tokenId, make.value.value, byteArrayOf()).withSender(makerSender).execute().verifySuccess()
            buyToken.mint(taker, buyTokenId, take.value.value, byteArrayOf()).withSender(takerSender).execute().verifySuccess()

            token.setApprovalForAll(transferProxy.address(), true).withSender(makerSender).execute().verifySuccess()
            buyToken.setApprovalForAll(transferProxy.address(), true).withSender(takerSender).execute().verifySuccess()

            assertEquals(make.value.value, token.balanceOf(maker, tokenId).call().awaitFirst())
            assertEquals(take.value.value, buyToken.balanceOf(taker, buyTokenId).call().awaitFirst())

            val orderData = OrderRaribleV2DataV1(listOf(), listOf())

            val makerOrderTuple = Tuple9(
                maker,
                Tuple2(make.type.forTx(), make.value.value),
                taker,
                Tuple2(take.type.forTx(), take.value.value),
                salt,
                BigInteger.ZERO,
                BigInteger.ZERO,
                orderData.getDataVersion(),
                orderData.toEthereum().bytes()
            )
            val takerOrderTuple = Tuple9(
                taker,
                Tuple2(take.type.forTx(), take.value.value),
                maker,
                Tuple2(make.type.forTx(), make.value.value),
                salt,
                BigInteger.ZERO,
                BigInteger.ZERO,
                orderData.getDataVersion(),
                orderData.toEthereum().bytes()
            )

            val makeSign = eip712Domain.hashToSign(Order.raribleExchangeV2Hash(
                maker = maker,
                make = make,
                taker = taker,
                take = take,
                salt = salt,
                start = 0,
                end = 0,
                data = orderData
            )).sign(makePrivateKey)
            val rightSign = eip712Domain.hashToSign(Order.raribleExchangeV2Hash(
                maker = taker,
                make = take,
                taker = maker,
                take = make,
                salt = salt,
                start = 0,
                end = 0,
                data = orderData
            )).sign(takePrivateKey)

            val receipt = exchangeV2Contract.matchOrders(
                makerOrderTuple,
                makeSign.bytes(),
                takerOrderTuple,
                rightSign.bytes()
            ).withSender(makerSender).execute().verifySuccess()

            val orderVersion = OrderVersion(
                maker = maker,
                taker = taker,
                make = make,
                take = take,
                type = OrderType.RARIBLE_V2,
                salt = EthUInt256.of(salt),
                data = orderData,
                start = null,
                end = null,
                signature = null,
                createdAt = nowMillis(),
                makePriceUsd = null,
                takePriceUsd = null,
                makePrice = null,
                takePrice = null,
                makeUsd = null,
                takeUsd = null
            )
            setField(pendingTransactionService, "exchangeContracts", hashSetOf(exchangeV2Contract.address()))

            orderUpdateService.save(orderVersion)

            processTransaction(receipt, expectedSize = 2)

            val makeHash = Order.hashKey(maker, make.type, take.type, salt)
            val takeHash = Order.hashKey(taker, take.type, make.type, salt)

            for (hash in listOf(makeHash, takeHash)) {
                val history = exchangeHistoryRepository.findLogEvents(hash, null).collectList().awaitFirst()
                assertThat(history).hasSize(1)
                assertThat(history.single().status).isEqualTo(LogEventStatus.PENDING)
                assertThat(history.single().data).isInstanceOf(OrderSideMatch::class.java)
            }

            val savedOrder = orderRepository.findById(orderVersion.hash)
            assertThat(savedOrder?.pending).hasSize(1)
            assertThat(savedOrder?.pending?.single()).isInstanceOf(OrderSideMatch::class.java)
        }
    }

    private suspend fun processTransaction(receipt: TransactionReceipt, expectedSize: Int = 1) {
        val tx = ethereum.ethGetTransactionByHash(receipt.transactionHash()).awaitFirst().get()

        val transactions = transactionApi.createOrderPendingTransaction(tx.toRequest()).collectList().awaitFirst()

        assertThat(transactions).hasSize(expectedSize)
        assertThat(transactions).allSatisfy {
            assertThat(it.transactionHash).isEqualTo(tx.hash())
            assertThat(it.address).isEqualTo(tx.to())
            assertThat(it.status).isEqualTo(LogEventDto.Status.PENDING)
        }
    }

    private fun Transaction.toRequest() = CreateTransactionRequestDto(
        hash = hash(),
        from = from(),
        input = input(),
        nonce = nonce().toLong(),
        to = to()
    )

    protected fun newSender(privateKey0: BigInteger? = null): Triple<Address, MonoSigningTransactionSender, BigInteger> {
        val (privateKey, _, address) = generateNewKeys(privateKey0)
        val sender = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            privateKey,
            BigInteger.valueOf(8000000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )
        return Triple(address, sender, privateKey)
    }

    private fun generateNewKeys(privateKey0: BigInteger? = null): NewKeys {
        val privateKey = privateKey0 ?: Numeric.toBigInt(RandomUtils.nextBytes(32))
        val publicKey = Sign.publicKeyFromPrivate(privateKey)
        val signer = Address.apply(Keys.getAddressFromPrivateKey(privateKey))
        return NewKeys(privateKey, publicKey, signer)
    }

    @TestConfiguration
    class TestAssetTypeServiceConfiguration {
        @Bean
        @Primary
        fun mockedAssetTypeService(): AssetTypeService {
            return mockk()
        }
    }
}
