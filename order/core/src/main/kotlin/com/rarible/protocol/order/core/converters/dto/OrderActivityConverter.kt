package com.rarible.protocol.order.core.converters.dto

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.listener.log.domain.LogEvent
import com.rarible.protocol.dto.*
import com.rarible.protocol.order.core.model.*
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.service.PriceNormalizer
import io.daonomic.rpc.domain.Word
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderActivityConverter(
    private val priceNormalizer: PriceNormalizer,
    private val orderRepository: OrderRepository
) {
    suspend fun convert(ar: ActivityResult): OrderActivityDto? {
        return when (ar) {
            is ActivityResult.History -> convertHistory(ar.value)
            is ActivityResult.Version -> convertVersion(ar.value)
        }
    }

    private suspend fun convertHistory(history: LogEvent): OrderActivityDto? {
        val transactionHash = history.transactionHash
        val blockHash = history.blockHash ?: DEFAULT_BLOCK_HASH
        val blockNumber = history.blockNumber ?: DEFAULT_BLOCK_NUMBER
        val logIndex = history.logIndex ?: DEFAULT_LOG_INDEX
        val data = history.data as OrderExchangeHistory

        if (data.maker == null || data.make == null || data.take == null) {
            return null
        }

        return when (data) {
            is OrderSideMatch -> {
                val leftOrder = orderRepository.findById(data.hash)
                val rightOrder = data.counterHash?.let { orderRepository.findById(it) }
                OrderActivityMatchDto(
                    id = history.id.toString(),
                    date = data.date,
                    left = OrderActivityMatchSideDto(
                        maker = data.maker,
                        asset = AssetDtoConverter.convert(data.make),
                        hash = data.hash,
                        type = leftOrder?.orderType
                    ),
                    right = OrderActivityMatchSideDto(
                        maker = data.taker,
                        asset = AssetDtoConverter.convert(data.take),
                        hash = data.counterHash ?: Word.apply(ByteArray(32)),
                        type = rightOrder?.orderType
                    ),
                    price = nftPrice(data.take, data.make),
                    priceUsd = data.takePriceUsd ?: data.makePriceUsd,
                    transactionHash = transactionHash,
                    blockHash = blockHash,
                    blockNumber = blockNumber,
                    logIndex = logIndex,
                    source = convert(data.source)
                )
            }
            is OrderCancel -> if (data.isBid()) {
                OrderActivityCancelBidDto(
                    id = history.id.toString(),
                    hash = data.hash,
                    maker = data.maker!!,
                    make = AssetTypeDtoConverter.convert(data.make!!.type),
                    take = AssetTypeDtoConverter.convert(data.take!!.type),
                    date = data.date,
                    transactionHash = transactionHash,
                    blockHash = blockHash,
                    blockNumber = blockNumber,
                    logIndex = logIndex,
                    source = convert(data.source)
                )
            } else {
                OrderActivityCancelListDto(
                    id = history.id.toString(),
                    hash = data.hash,
                    maker = data.maker!!,
                    make = AssetTypeDtoConverter.convert(data.make!!.type),
                    take = AssetTypeDtoConverter.convert(data.take!!.type),
                    date = data.date,
                    transactionHash = transactionHash,
                    blockHash = blockHash,
                    blockNumber = blockNumber,
                    logIndex = logIndex,
                    source = convert(data.source)
                )
            }
            is OrderNew -> if (data.isBid()) {
                OrderActivityBidDto(
                    date = data.date,
                    id = history.id.toString(),
                    hash = data.hash,
                    maker = data.maker,
                    make = AssetDtoConverter.convert(data.make),
                    take = AssetDtoConverter.convert(data.take),
                    price = nftPrice(data.make, data.take),
                    source = convert(data.source),
                    priceUsd = null // TODO[punk]: not sure.
                )
            } else {
                OrderActivityListDto(
                    date = data.date,
                    id = history.id.toString(),
                    hash = data.hash,
                    maker = data.maker,
                    make = AssetDtoConverter.convert(data.make),
                    take = AssetDtoConverter.convert(data.take),
                    price = nftPrice(data.take, data.make),
                    source = convert(data.source),
                    priceUsd = null // TODO[punk]: not sure.
                )
            }
        }
    }

    private suspend fun convertVersion(version: OrderVersion): OrderActivityDto {
        return when {
            version.isBid() -> OrderActivityBidDto(
                date = version.createdAt,
                id = version.id.toString(),
                hash = version.hash,
                maker = version.maker,
                make = AssetDtoConverter.convert(version.make),
                take = AssetDtoConverter.convert(version.take),
                price = price(version.make, version.take),
                priceUsd = version.takePriceUsd ?: version.makePriceUsd,
                source = convert(version.platform)
            )
            else -> OrderActivityListDto(
                date = version.createdAt,
                id = version.id.toString(),
                hash = version.hash,
                maker = version.maker,
                make = AssetDtoConverter.convert(version.make),
                take = AssetDtoConverter.convert(version.take),
                price = price(version.take, version.make),
                priceUsd = version.takePriceUsd ?: version.makePriceUsd,
                source = convert(version.platform)
            )
        }
    }

    private fun convert(source: HistorySource): OrderActivityDto.Source = when (source) {
        HistorySource.RARIBLE -> OrderActivityDto.Source.RARIBLE
        HistorySource.OPEN_SEA -> OrderActivityDto.Source.OPEN_SEA
        HistorySource.CRYPTO_PUNKS -> OrderActivityDto.Source.CRYPTO_PUNKS
    }

    private fun convert(platform: Platform): OrderActivityDto.Source = when (platform) {
        Platform.RARIBLE -> OrderActivityDto.Source.RARIBLE
        Platform.OPEN_SEA -> OrderActivityDto.Source.OPEN_SEA
        Platform.CRYPTO_PUNKS -> OrderActivityDto.Source.CRYPTO_PUNKS
    }

    private suspend fun nftPrice(left: Asset, right: Asset): BigDecimal {
        return if (left.type.nft) {
            price(right, left)
        } else {
            price(left, right)
        }
    }

    private suspend fun price(left: Asset, right: Asset): BigDecimal {
        return if (right.value != EthUInt256.ZERO) {
            priceNormalizer.normalize(left) / priceNormalizer.normalize(right)
        } else BigDecimal.ZERO
    }

    private companion object {
        val DEFAULT_BLOCK_HASH: Word = Word.apply(ByteArray(32))
        const val DEFAULT_BLOCK_NUMBER: Long = 0
        const val DEFAULT_LOG_INDEX: Int = 0
    }
}

private val Order.orderType: OrderActivityMatchSideDto.Type
    get() = if (this.take.type.nft)
        OrderActivityMatchSideDto.Type.BID
    else
        OrderActivityMatchSideDto.Type.SELL
