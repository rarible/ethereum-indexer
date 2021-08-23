package com.rarible.protocol.order.core.model

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.contracts.Tuples
import com.rarible.protocol.contracts.Tuples.keccak256
import com.rarible.protocol.order.core.misc.zeroWord
import com.rarible.protocol.order.core.repository.order.MongoOrderRepository
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.mapping.Document
import scala.Tuple10
import scala.Tuple3
import scala.Tuple4
import scala.Tuple9
import scalether.abi.Uint256Type
import scalether.abi.Uint8Type
import scalether.domain.Address
import scalether.util.Hash
import scalether.util.Hex
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

@Document(MongoOrderRepository.COLLECTION)
data class Order(
    val maker: Address,
    val taker: Address?,

    val make: Asset,
    val take: Asset,

    val type: OrderType,

    val fill: EthUInt256,
    val cancelled: Boolean,

    val makeStock: EthUInt256,

    val salt: EthUInt256,
    val start: Long?,
    val end: Long?,
    val data: OrderData,
    val signature: Binary?,

    val createdAt: Instant,
    val lastUpdateAt: Instant,

    val pending: List<OrderExchangeHistory> = emptyList(),

    val makePriceUsd: BigDecimal? = null,
    val takePriceUsd: BigDecimal? = null,
    val makeUsd: BigDecimal? = null,
    val takeUsd: BigDecimal? = null,
    val priceHistory: List<OrderPriceHistoryRecord> = emptyList(),

    val platform: Platform = Platform.RARIBLE,

    @Id
    val hash: Word = hashKey(maker, make.type, take.type, salt.value),

    @Version
    val version: Long? = null
) {
    fun forV1Tx() = run {
        assert(type == OrderType.RARIBLE_V1)
        assert(data is OrderDataLegacy)
        val makeLegacy = make.type.toLegacy()
            ?: throw IllegalArgumentException("not supported asset type for V1 order: ${make.type}")
        val takeLegacy = take.type.toLegacy()
            ?: throw IllegalArgumentException("not supported asset type for V1 order: ${take.type}")
        val orderKey = Tuple4(maker, salt.value, makeLegacy.toTuple(), takeLegacy.toTuple())
        Tuple4(orderKey, make.value.value, take.value.value, (data as OrderDataLegacy).fee.toBigInteger())
    }

    private fun LegacyAssetType.toTuple() = Tuple3(token, tokenId, clazz.value)

    fun forTx(wrongDataEncode: Boolean = false) = Tuple9(
        maker,
        make.forTx(),
        taker ?: Address.ZERO(),
        take.forTx(),
        salt.value,
        start?.toBigInteger() ?: BigInteger.ZERO,
        end?.toBigInteger() ?: BigInteger.ZERO,
        data.getDataVersion(),
        data.toEthereum(wrongDataEncode).bytes()
    )

    //fun hash(): Word = hash(this)

    fun withMakeBalance(makeBalance: EthUInt256, protocolCommission: EthUInt256): Order {
        return copy(makeStock = calculateMakeStock(
            make.value,
            take.value,
            fill, data,
            makeBalance,
            protocolCommission,
            getFeeSide(make.type, take.type),
            cancelled
        ))
    }

    fun withNewValues(
        make: EthUInt256,
        take: EthUInt256,
        makeStock: EthUInt256,
        signature: Binary?,
        updateAt: Instant
    ): Order {
        return copy(
            make = this.make.copy(value = make),
            take = this.take.copy(value = take),
            makeStock = makeStock,
            signature = signature,
            lastUpdateAt = getLatestLastUpdateAt(updateAt)
        )
    }

    fun withFillAndCancelledAndPendingAndChangeDate(
        fill: EthUInt256,
        makeBalance: EthUInt256,
        protocolCommission: EthUInt256,
        cancelled: Boolean,
        pending: List<OrderExchangeHistory>,
        changeDate: Instant
    ): Order {
        return copy(
            fill = fill,
            cancelled = cancelled,
            makeStock = calculateMakeStock(
                make.value,
                take.value,
                fill,
                data,
                makeBalance,
                protocolCommission,
                getFeeSide(make.type, take.type),
                cancelled
            ),
            pending = pending,
            lastUpdateAt = getLatestLastUpdateAt(changeDate)
        )
    }

    fun withOrderUsdValue(usdValue: OrderUsdValue): Order {
        return copy(
            takePriceUsd = usdValue.takePriceUsd,
            makePriceUsd = usdValue.makePriceUsd,
            takeUsd = usdValue.takeUsd,
            makeUsd = usdValue.makeUsd
        )
    }

    fun withTakePrice(price: BigDecimal?): Order {
        return copy(takePriceUsd = price)
    }

    fun withMakePrice(price: BigDecimal?): Order {
        return copy(makePriceUsd = price)
    }

    private fun getLatestLastUpdateAt(lastUpdateAt: Instant): Instant {
        return if (lastUpdateAt > this.lastUpdateAt) lastUpdateAt else this.lastUpdateAt
    }

    companion object {
        fun calculateMakeStock(
            makeValue: EthUInt256,
            takeValue: EthUInt256,
            fill: EthUInt256,
            data: OrderData,
            makeBalance: EthUInt256,
            protocolCommission: EthUInt256,
            feeSide: FeeSide,
            cancelled: Boolean
        ): EthUInt256 {
            val (make) = calculateRemaining(makeValue, takeValue, fill, cancelled)
            val fee = if (feeSide == FeeSide.MAKE) calculateFee(data, protocolCommission) else EthUInt256.ZERO

            val roundedMakeBalance = calculateRoundedMakeBalance(
                makeValue = makeValue,
                takeValue = takeValue,
                makeBalance = (makeBalance * EthUInt256.of(10000)) / (fee + EthUInt256.of(10000))
            )
            return minOf(make, roundedMakeBalance)
        }

        private fun calculateRemaining(
            makeValue: EthUInt256,
            takeValue: EthUInt256,
            fill: EthUInt256,
            cancelled: Boolean
        ): Pair<EthUInt256, EthUInt256> {
            return if (cancelled) {
                EthUInt256.ZERO to EthUInt256.ZERO
            } else {
                val take = takeValue - fill
                val make = take * makeValue / takeValue
                make to take
            }
        }

        private fun calculateFee(data: OrderData, protocolCommission: EthUInt256): EthUInt256 {
            return when (data) {
                is OrderRaribleV2DataV1 -> data.originFees.fold(protocolCommission) { acc, part -> acc + part.value  }
                is OrderDataLegacy -> EthUInt256.of(data.fee.toLong())
                is OrderOpenSeaV1DataV1 -> EthUInt256.ZERO
            }
        }

        private fun calculateRoundedMakeBalance(
            makeValue: EthUInt256,
            takeValue: EthUInt256,
            makeBalance: EthUInt256
        ): EthUInt256 {
            val maxTake = makeBalance * takeValue / makeValue
            return makeValue * maxTake / takeValue
        }

        fun hash(order: Order): Word = with(order) {
            hash(maker, make, taker, take, salt.value, start, end, data, type)
        }

        fun hash(
            maker: Address,
            make: Asset,
            taker: Address?,
            take: Asset,
            salt: BigInteger,
            start: Long?,
            end: Long?,
            data: OrderData,
            type: OrderType
        ): Word {
            return when (type) {
                OrderType.RARIBLE_V2 -> raribleExchangeV2Hash(maker, make, taker, take, salt, start, end, data)
                OrderType.RARIBLE_V1 -> raribleExchangeV1Hash(maker, make,  take, salt, data)
                OrderType.OPEN_SEA_V1 -> openSeaV1Hash(maker, make, taker, take, salt, start, end, data)
            }
        }

        fun Order.legacyMessage(): String {
            return legacyMessage(maker, make, take, salt.value, data)
        }

        fun legacyMessage(maker: Address, make: Asset, take: Asset, salt: BigInteger, data: OrderData): String {
            val legacyMakeAsset = make.type.toLegacy() ?: error("Unsupported make asset ${make.type} by legacy contract")
            val legacyTakeAsset = take.type.toLegacy() ?: error("Unsupported take asset ${take.type} by legacy contract")
            val legacyData = (data as? OrderDataLegacy) ?: error("Unsupported data for legacy contract")

            val binary = Tuples.legacyOrderHashType().encode(
                Tuple4(
                    Tuple4(
                        maker,
                        salt,
                        Tuple3(
                            legacyMakeAsset.token,
                            legacyMakeAsset.tokenId,
                            legacyMakeAsset.clazz.value
                        ),
                        Tuple3(
                            legacyTakeAsset.token,
                            legacyTakeAsset.tokenId,
                            legacyTakeAsset.clazz.value
                        )
                    ),
                    make.value.value,
                    take.value.value,
                    legacyData.fee.toBigInteger()
                )
            )
            val hash = Hash.sha3(binary.bytes())
            return Hex.to(hash)
        }

        fun raribleExchangeV1Hash(
            maker: Address,
            make: Asset,
            take: Asset,
            salt: BigInteger,
            data: OrderData
        ): Word {
            val legacyMakeAsset = make.type.toLegacy() ?: error("Unsupported make asset ${make.type} by legacy contract")
            val legacyTakeAsset = take.type.toLegacy() ?: error("Unsupported take asset ${take.type} by legacy contract")
            val legacyData = (data as? OrderDataLegacy) ?: error("Unsupported data for legacy contract")

            val binary = Tuples.legacyOrderHashType().encode(
                Tuple4(
                    Tuple4(
                        maker,
                        salt,
                        Tuple3(
                            legacyMakeAsset.token,
                            legacyMakeAsset.tokenId,
                            legacyMakeAsset.clazz.value
                        ),
                        Tuple3(
                            legacyTakeAsset.token,
                            legacyTakeAsset.tokenId,
                            legacyTakeAsset.clazz.value
                        )
                    ),
                    make.value.value,
                    take.value.value,
                    legacyData.fee.toBigInteger()
                )
            )
            val hash = Hash.sha3(binary.bytes())
            return Word.apply(hash)
        }

        fun raribleExchangeV2Hash(
            maker: Address,
            make: Asset,
            taker: Address?,
            take: Asset,
            salt: BigInteger,
            start: Long?,
            end: Long?,
            data: OrderData
        ): Word {
            return keccak256(
                Tuples.orderHashType().encode(
                    Tuple10(
                        TYPE_HASH.bytes(),
                        maker,
                        Asset.hash(make).bytes(),
                        taker ?: Address.ZERO(),
                        Asset.hash(take).bytes(),
                        salt,
                        (start ?: 0).toBigInteger(),
                        (end ?: 0).toBigInteger(),
                        data.getDataVersion(),
                        keccak256(data.toEthereum()).bytes()
                    )
                )
            )
        }

        fun openSeaV1Hash(
            maker: Address,
            make: Asset,
            taker: Address?,
            take: Asset,
            salt: BigInteger,
            start: Long?,
            end: Long?,
            data: OrderData
        ): Word {
            val openSeaData = (data as? OrderOpenSeaV1DataV1) ?: error("Unsupported data type ${data.javaClass} for OpenSea contract")
            val nftAsset = when {
                make.type.nft -> make.type
                take.type.nft -> take.type
                else -> throw UnsupportedOperationException("Unsupported exchange assets pairs, can't find NFT asset type")
            }
            val paymentAsset = when {
                make.type.nft.not() -> make
                take.type.nft.not() -> take
                else -> throw UnsupportedOperationException("Unsupported exchange assets pairs, can't find payment asset type")
            }
            return openSeaV1Hash(
                maker = maker,
                taker = taker,
                nftToken = nftAsset.token,
                paymentToken = paymentAsset.type.token,
                basePrice = paymentAsset.value.value,
                salt = salt,
                start = start,
                end = end,
                data = openSeaData
            )
        }

        fun openSeaV1Hash(
            maker: Address,
            taker: Address?,
            nftToken: Address,
            paymentToken: Address,
            basePrice: BigInteger,
            salt: BigInteger,
            start: Long?,
            end: Long?,
            data: OrderOpenSeaV1DataV1
        ): Word {
            return keccak256(
                data.exchange
                    .add(maker)
                    .add(taker ?: Address.ZERO())
                    .add(Uint256Type.encode(data.makerRelayerFee))
                    .add(Uint256Type.encode(data.takerRelayerFee))
                    .add(Uint256Type.encode(data.makerProtocolFee))
                    .add(Uint256Type.encode(data.takerProtocolFee))
                    .add(data.feeRecipient)
                    .add(Uint8Type.encode(data.feeMethod.value).bytes().sliceArray(31..31))
                    .add(Uint8Type.encode(data.side.value).bytes().sliceArray(31..31))
                    .add(Uint8Type.encode(data.saleKind.value).bytes().sliceArray(31..31))
                    .add(nftToken)
                    .add(Uint8Type.encode(data.howToCall.value).bytes().sliceArray(31..31))
                    .add(data.callData)
                    .add(data.replacementPattern)
                    .add(data.staticTarget)
                    .add(data.staticExtraData)
                    .add(paymentToken)
                    .add(Uint256Type.encode(basePrice))
                    .add(Uint256Type.encode(data.extra))
                    .add(Uint256Type.encode(start?.toBigInteger() ?: BigInteger.ZERO))
                    .add(Uint256Type.encode(end?.toBigInteger() ?: BigInteger.ZERO))
                    .add(Uint256Type.encode(salt))
            )
        }

        fun hashKey(maker: Address, makeAssetType: AssetType, takeAssetType: AssetType, salt: BigInteger): Word =
            keccak256(
                Tuples.orderKeyHashType().encode(
                    Tuple4(
                        maker,
                        AssetType.hash(makeAssetType).bytes(),
                        AssetType.hash(takeAssetType).bytes(),
                        salt
                    )
                )
            )

        private val TYPE_HASH =
            keccak256("Order(address maker,Asset makeAsset,address taker,Asset takeAsset,uint256 salt,uint256 start,uint256 end,bytes4 dataType,bytes data)Asset(AssetType assetType,uint256 value)AssetType(bytes4 assetClass,bytes data)")

        fun getFeeSide(make: AssetType, take: AssetType): FeeSide {
            return when {
                make is EthAssetType -> FeeSide.MAKE
                take is EthAssetType -> FeeSide.TAKE
                make is Erc20AssetType -> FeeSide.MAKE
                take is Erc20AssetType -> FeeSide.TAKE
                make is Erc1155AssetType -> FeeSide.MAKE
                take is Erc1155AssetType -> FeeSide.TAKE
                else -> FeeSide.NONE
            }
        }
    }
}

fun Order.invert(maker: Address, amount: BigInteger, newSalt: Word = zeroWord()) = run {
    val (makeValue, takeValue) = calculateAmounts(make.value.value, take.value.value, amount, isBid())
    this.copy(
        maker = maker,
        taker = this.maker,
        make = take,
        take = make,
        hash = Order.hashKey(maker, take.type, make.type, salt.value),
        salt = EthUInt256.of(newSalt)
    ).withNewValues(EthUInt256(makeValue), EthUInt256(takeValue), EthUInt256.ZERO, null, nowMillis())
}

fun Order.isBid() = take.type.nft

fun calculateAmounts(
    make: BigInteger,
    take: BigInteger,
    amount: BigInteger,
    bid: Boolean
): Pair<BigInteger, BigInteger> {
    return if (bid) {
        Pair(amount, amount.multiply(make).div(take))
    } else {
        Pair(amount.multiply(take).div(make), amount)
    }
}

val AssetType.token: Address
    get() {
        return when (this) {
            is Erc721AssetType -> token
            is Erc1155AssetType -> token
            is Erc1155LazyAssetType -> token
            is Erc20AssetType -> token
            is Erc721LazyAssetType -> token
            is EthAssetType -> Address.ZERO()
        }
    }
