package com.rarible.protocol.order.core.service

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.contracts.Tuples
import com.rarible.protocol.contracts.exchange.v2.ExchangeV2
import com.rarible.protocol.contracts.exchange.v2.events.MatchEvent
import com.rarible.protocol.contracts.exchange.metatx.EIP712MetaTransaction
import com.rarible.protocol.order.core.configuration.OrderIndexerProperties
import com.rarible.protocol.order.core.misc.methodSignatureId
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.model.RaribleMatchedOrders.SimpleOrder
import com.rarible.protocol.order.core.trace.TraceCallService
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import org.springframework.stereotype.Component
import scalether.domain.Address
import java.math.BigInteger

@Component
class RaribleExchangeV2OrderParser(
    private val exchangeContractAddresses: OrderIndexerProperties.ExchangeContractAddresses,
    private val traceCallService: TraceCallService,
    private val featureFlags: OrderIndexerProperties.FeatureFlags
) {

    suspend fun parseMatchedOrders(txHash: Word, txInput: Binary, event: MatchEvent): RaribleMatchedOrders? {
        val inputs = getInputs(txHash, txInput)

        val leftAssetType = event.leftAsset().toAssetType()
        val rightAssetType = event.rightAsset().toAssetType()

        return inputs.map { parseOrders(it) }.firstOrNull { orders ->
            val leftHash = Order.hashKey(
                event.leftMaker(),
                if (orders.left.makeAssetType.isCollection) leftAssetType.tryToConvertInCollection() else leftAssetType,
                if (orders.left.takeAssetType.isCollection) rightAssetType.tryToConvertInCollection() else rightAssetType,
                orders.left.salt.value,
                orders.left.data
            )
            val rightHash = Order.hashKey(
                event.rightMaker(),
                if (orders.right.makeAssetType.isCollection) rightAssetType.tryToConvertInCollection() else rightAssetType,
                if (orders.right.takeAssetType.isCollection) leftAssetType.tryToConvertInCollection() else leftAssetType,
                orders.right.salt.value,
                orders.right.data
            )
            Word.apply(event.leftHash()) == leftHash && Word.apply(event.rightHash()) == rightHash
        }
    }

    suspend fun getInputs(txHash: Word, txInput: Binary): List<Binary> {
        val matchOrderSignature = ExchangeV2.matchOrdersSignature().id()
        val metaTransactionSignature = EIP712MetaTransaction.executeMetaTransactionSignature().id()
        return if (txInput.methodSignatureId() in setOf(matchOrderSignature, metaTransactionSignature)) {
            listOf(txInput)
        } else if (featureFlags.skipGetTrace) {
            emptyList()
        } else {
            traceCallService.findAllRequiredCallInputs(
                txHash,
                txInput,
                exchangeContractAddresses.v2,
                ExchangeV2.matchOrdersSignature().id()
            )
        }
    }

    fun parseOrders(input: Binary): RaribleMatchedOrders {
        return if (input.methodSignatureId() == EIP712MetaTransaction.executeMetaTransactionSignature().id()) {
            parseMetaTransaction(input)
        } else {
            parseMatchedOrders(input)
        }
    }

    fun parseMatchedOrders(input: Binary): RaribleMatchedOrders {
        val signature = ExchangeV2.matchOrdersSignature()
        val decoded = signature.`in`().decode(input, 4)
        return RaribleMatchedOrders(
            left = SimpleOrder(
                makeAssetType = decoded.value()._1()._2()._1().toAssetType(),
                takeAssetType = decoded.value()._1()._4()._1().toAssetType(),
                data = convertOrderData(
                    version = Binary.apply(decoded.value()._1()._8()),
                    data = Binary.apply(decoded.value()._1()._9())
                ),
                salt = EthUInt256.of(decoded.value()._1()._5())
            ),
            right = SimpleOrder(
                makeAssetType = decoded.value()._3()._2()._1().toAssetType(),
                takeAssetType = decoded.value()._3()._4()._1().toAssetType(),
                data = convertOrderData(
                    version = Binary.apply(decoded.value()._3()._8()),
                    data = Binary.apply(decoded.value()._3()._9())
                ),
                salt = EthUInt256.of(decoded.value()._3()._5())
            )
        )
    }

    private fun parseMetaTransaction(input: Binary): RaribleMatchedOrders {
        val signature = EIP712MetaTransaction.executeMetaTransactionSignature()
        val decoded = signature.`in`().decode(input, 4)
        return parseMatchedOrders(Binary.apply(decoded.value()._2()))
    }

    fun parseOnChainOrder(data: Binary): OnChainOrder {
        val orderInput = ExchangeV2.upsertOrderSignature().`in`().decode(data, 0).value()
        val maker = orderInput._1()
        val makeAssetType = orderInput._2()._1().toAssetType()
        val makeValue = EthUInt256.of(orderInput._2()._2())
        val make = Asset(makeAssetType, makeValue)

        val taker = orderInput._3().takeUnless { it == Address.ZERO() }
        val takeAssetType = orderInput._4()._1().toAssetType()
        val takeValue = EthUInt256.of(orderInput._4()._2())
        val take = Asset(takeAssetType, takeValue)

        val salt = EthUInt256.of(orderInput._5())
        val start = orderInput._6().takeUnless { it == BigInteger.ZERO }?.toLong()
        val end = orderInput._7().takeUnless { it == BigInteger.ZERO }?.toLong()
        val orderData = convertOrderData(Binary.apply(orderInput._8()), Binary.apply(orderInput._9()))
        val createdAt = nowMillis()
        return OnChainOrder(
            maker = maker,
            taker = taker,
            make = make,
            take = take,
            createdAt = createdAt,
            date = createdAt,
            platform = Platform.RARIBLE,
            salt = salt,
            start = start,
            end = end,
            data = orderData,
            signature = null,
            orderType = OrderType.RARIBLE_V2,
            priceUsd = null,
            source = HistorySource.RARIBLE,
            hash = Order.hashKey(maker, makeAssetType, takeAssetType, salt.value)
        )
    }

    fun convertOrderData(version: Binary, data: Binary): OrderData {
        return when (version) {
            OrderDataVersion.RARIBLE_V2_DATA_V1.ethDataType -> {
                val (payouts, originFees) = when {
                    data.slice(0, 32) == ENCODED_ORDER_RARIBLE_V2_DATA_V1_PREFIX -> {
                        val decoded = Tuples.orderDataV1Type().decode(data, 0)
                        decoded.value()._1().map { it.toPart() } to decoded.value()._2().map { it.toPart() }
                    }
                    data.slice(0, 32) == WRONG_ENCODED_ORDER_RARIBLE_V2_DATA_V1_PREFIX -> {
                        val decodedWrong = Tuples.wrongOrderDataV1Type().decode(data, 0)
                        decodedWrong.value()._1().map { it.toPart() } to decodedWrong.value()._2().map { it.toPart() }
                    }
                    else -> throw IllegalArgumentException("Unsupported data encode (data=$data)")
                }
                OrderRaribleV2DataV1(
                    payouts = payouts,
                    originFees = originFees
                )
            }
            OrderDataVersion.RARIBLE_V2_DATA_V2.ethDataType -> {
                val decoded = Tuples.orderDataV2Type().decode(data, 0).value()
                val payouts = decoded._1().map { it.toPart() }
                val originFees = decoded._2().map { it.toPart() }
                val isMakeFill = decoded._3() > BigInteger.ZERO
                OrderRaribleV2DataV2(
                    payouts = payouts,
                    originFees = originFees,
                    isMakeFill = isMakeFill
                )
            }
            else -> throw IllegalArgumentException("Unsupported order data version $version")
        }
    }

    private fun AssetType.tryToConvertInCollection(): AssetType {
        return when (this) {
            is NftAssetType -> CollectionAssetType(token)
            is CollectionAssetType,
            is Erc20AssetType,
            is EthAssetType,
            is GenerativeArtAssetType -> this
        }
    }

    private val AssetType.isCollection: Boolean
        get() = this is CollectionAssetType

    private companion object {
        val WRONG_ENCODED_ORDER_RARIBLE_V2_DATA_V1_PREFIX: Binary =
            Binary.apply("0x0000000000000000000000000000000000000000000000000000000000000040")
        val ENCODED_ORDER_RARIBLE_V2_DATA_V1_PREFIX: Binary =
            Binary.apply("0x0000000000000000000000000000000000000000000000000000000000000020")
    }
}
