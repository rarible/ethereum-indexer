package com.rarible.protocol.order.listener.service.descriptors.exchange.opensea

import com.rarible.core.common.nowMillis
import com.rarible.core.test.wait.Wait
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.contracts.exchange.wyvern.OrdersMatchedEvent
import com.rarible.protocol.dto.OrderActivityMatchDto
import com.rarible.protocol.dto.PrepareOrderTxFormDto
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.service.CallDataEncoder
import com.rarible.protocol.order.core.service.CommonSigner
import com.rarible.protocol.order.core.service.PrepareTxService
import com.rarible.protocol.order.listener.integration.IntegrationTest
import com.rarible.protocol.order.listener.misc.sign
import io.daonomic.rpc.domain.Binary
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import scalether.domain.Address
import scalether.domain.request.Transaction
import java.math.BigInteger

@FlowPreview
@IntegrationTest
internal class WyvernExchangeOrderMatchDescriptorTest : AbstractOpenSeaV1Test() {
    @Autowired
    private lateinit var prepareTxService: PrepareTxService
    @Autowired
    private lateinit var callDataEncoder: CallDataEncoder
    @Autowired
    private lateinit var commonSigner: CommonSigner

    @Test
    fun `should execute sell order`() = runBlocking<Unit>  {
        val sellMaker = userSender1.from()
        val buyMaker = userSender2.from()
        val target = token721.address()
        val tokenId = EthUInt256.ONE
        val paymentToken = token1.address()

        token721.mint(sellMaker, BigInteger.ONE, "test").execute().verifySuccess()
        token1.mint(buyMaker, BigInteger.TEN.pow(10)).execute().verifySuccess()

        val sellTransfer = Transfer(
            type = Transfer.Type.ERC721,
            from = sellMaker,
            to = Address.ZERO(),
            tokenId = tokenId.value,
            value = BigInteger.ONE,
            data = Binary.apply()
        )
        val sellCallData = callDataEncoder.encodeTransferCallData(sellTransfer)

        val sellOrder = Order(
            maker = sellMaker,
            taker = null,
            make = Asset(Erc721AssetType(target, tokenId), EthUInt256.ONE),
            take = Asset(Erc20AssetType(paymentToken), EthUInt256.TEN),
            makeStock = EthUInt256.ONE,
            type = OrderType.OPEN_SEA_V1,
            fill = EthUInt256.ZERO,
            cancelled = false,
            salt = EthUInt256.TEN,
            start = nowMillis().epochSecond - 10,
            end = null,
            signature = null,
            data = OrderOpenSeaV1DataV1(
                exchange = exchange.address(),
                makerRelayerFee = BigInteger.valueOf(250),
                takerRelayerFee = BigInteger.ZERO,
                makerProtocolFee = BigInteger.ZERO,
                takerProtocolFee = BigInteger.ZERO,
                feeRecipient = exchange.address(),
                feeMethod = OpenSeaOrderFeeMethod.SPLIT_FEE,
                side = OpenSeaOrderSide.SELL,
                saleKind = OpenSeaOrderSaleKind.FIXED_PRICE,
                howToCall = OpenSeaOrderHowToCall.CALL,
                callData = sellCallData.callData,
                replacementPattern = sellCallData.replacementPattern,
                staticTarget = Address.ZERO(),
                staticExtraData = Binary.apply(),
                extra = BigInteger.ZERO
            ),
            createdAt = nowMillis(),
            lastUpdateAt = nowMillis()
        )
        val hash = Order.hash(sellOrder)
        val hashToSign = commonSigner.openSeaHashToSign(hash)
        logger.info("Sell order hash: $hash, hash to sing: $hashToSign")

        val signature = hashToSign.sign(privateKey1)

        val signedSellOrder = sellOrder.copy(signature = signature, hash = hash)
        orderRepository.save(signedSellOrder)

        val form = PrepareOrderTxFormDto(
            maker = buyMaker,
            amount = BigInteger.TEN,
            originFees = emptyList(),
            payouts = emptyList()
        )
        val response = prepareTxService.prepareTransaction(signedSellOrder, form)

        userSender2.sendTransaction(
            Transaction(
                exchange.address(),
                userSender2.from(),
                8000000.toBigInteger(),
                BigInteger.ONE,
                BigInteger.ZERO,
                response.transaction.data,
                null
            )
        ).verifySuccess()

        Wait.waitAssert {
            val items = exchangeHistoryRepository.findByItemType(ItemType.ORDER_SIDE_MATCH).collectList().awaitFirst()
            assertThat(items).hasSize(2)

            val map = items
                .map { it.data as OrderSideMatch }
                .associateBy { it.side }

            val left = map[OrderSide.LEFT]
            val right = map[OrderSide.RIGHT]

            assertThat(left?.fill).isEqualTo(EthUInt256.TEN)
            assertThat(right?.fill).isEqualTo(EthUInt256.ONE)

            assertThat(left?.make)
                .isEqualTo(signedSellOrder.make)
            assertThat(left?.take)
                .isEqualTo(signedSellOrder.take)
            assertThat(left?.externalOrderExecutedOnRarible).isTrue()

            assertThat(right?.make)
                .isEqualTo(signedSellOrder.take)
            assertThat(right?.take)
                .isEqualTo(signedSellOrder.make)
            assertThat(right?.externalOrderExecutedOnRarible).isTrue()

            val filledOrder = orderRepository.findById(signedSellOrder.hash)
            assertThat(filledOrder?.fill).isEqualTo(EthUInt256.TEN)

            checkActivityWasPublished(signedSellOrder, OrdersMatchedEvent.id(), OrderActivityMatchDto::class.java)
        }
    }

    @Test
    fun `should execute buy order`() = runBlocking<Unit>  {
        val sellMaker = userSender1.from()
        val buyMaker = userSender2.from()
        val target = token721.address()
        val tokenId = EthUInt256.ONE
        val paymentToken = token1.address()

        token721.mint(sellMaker, BigInteger.ONE, "test").execute().verifySuccess()
        token1.mint(buyMaker, BigInteger.TEN.pow(10)).execute().verifySuccess()
        token1.mint(sellMaker, BigInteger.TEN.pow(10)).execute().verifySuccess()

        val buyTransfer = Transfer(
            type = Transfer.Type.ERC721,
            from = Address.ZERO(),
            to = buyMaker,
            tokenId = tokenId.value,
            value = BigInteger.ONE,
            data = Binary.apply()
        )
        val buyCallData = callDataEncoder.encodeTransferCallData(buyTransfer)

        val buyOrder = Order(
            maker = sellMaker,
            taker = null,
            make = Asset(Erc20AssetType(paymentToken), EthUInt256.TEN),
            take = Asset(Erc721AssetType(target, tokenId), EthUInt256.ONE),
            makeStock = EthUInt256.TEN,
            type = OrderType.OPEN_SEA_V1,
            fill = EthUInt256.ZERO,
            cancelled = false,
            salt = EthUInt256.TEN,
            start = nowMillis().epochSecond - 10,
            end = null,
            signature = null,
            data = OrderOpenSeaV1DataV1(
                exchange = exchange.address(),
                makerRelayerFee = BigInteger.ZERO,
                takerRelayerFee = BigInteger.valueOf(250),
                makerProtocolFee = BigInteger.ZERO,
                takerProtocolFee = BigInteger.ZERO,
                feeRecipient = exchange.address(),
                feeMethod = OpenSeaOrderFeeMethod.SPLIT_FEE,
                side = OpenSeaOrderSide.BUY,
                saleKind = OpenSeaOrderSaleKind.FIXED_PRICE,
                howToCall = OpenSeaOrderHowToCall.CALL,
                callData =buyCallData.callData,
                replacementPattern = buyCallData.replacementPattern,
                staticTarget = Address.ZERO(),
                staticExtraData = Binary.apply(),
                extra = BigInteger.ZERO
            ),
            createdAt = nowMillis(),
            lastUpdateAt = nowMillis()
        )
        val hash = Order.hash(buyOrder)
        val hashToSign = commonSigner.openSeaHashToSign(hash)
        logger.info("Buy order hash: $hash, hash to sing: $hashToSign")

        val signature = hashToSign.sign(privateKey1)

        val signedBuyOrder = buyOrder.copy(signature = signature, hash = hash)
        orderRepository.save(signedBuyOrder)

        val form = PrepareOrderTxFormDto(
            maker = sellMaker,
            amount = BigInteger.ONE,
            originFees = emptyList(),
            payouts = emptyList()
        )
        val response = prepareTxService.prepareTransaction(signedBuyOrder, form)

        userSender1.sendTransaction(
            Transaction(
                exchange.address(),
                userSender1.from(),
                8000000.toBigInteger(),
                BigInteger.ONE,
                BigInteger.ZERO,
                response.transaction.data,
                null
            )
        ).verifySuccess()

        Wait.waitAssert {
            val items = exchangeHistoryRepository.findByItemType(ItemType.ORDER_SIDE_MATCH).collectList().awaitFirst()
            assertThat(items).hasSize(2)

            val map = items
                .map { it.data as OrderSideMatch }
                .associateBy { it.side }

            val left = map[OrderSide.LEFT]
            val right = map[OrderSide.RIGHT]

            assertThat(left?.fill).isEqualTo(EthUInt256.ONE)
            assertThat(right?.fill).isEqualTo(EthUInt256.TEN)

            assertThat(left?.make)
                .isEqualTo(signedBuyOrder.make)
            assertThat(left?.take)
                .isEqualTo(signedBuyOrder.take)
            assertThat(left?.externalOrderExecutedOnRarible).isTrue()

            assertThat(right?.make)
                .isEqualTo(signedBuyOrder.take)
            assertThat(right?.take)
                .isEqualTo(signedBuyOrder.make)
            assertThat(right?.externalOrderExecutedOnRarible).isTrue()

            val filledOrder = orderRepository.findById(signedBuyOrder.hash)
            assertThat(filledOrder?.fill).isEqualTo(EthUInt256.ONE)

            checkActivityWasPublished(signedBuyOrder, OrdersMatchedEvent.id(), OrderActivityMatchDto::class.java)
        }
    }
}

