package com.rarible.protocol.order.core.model

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.order.core.repository.order.OrderVersionRepository
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import scalether.domain.Address
import java.math.BigDecimal
import java.time.Instant

@Document(collection = OrderVersionRepository.COLLECTION)
data class OrderVersion(
    val hash: Word,
    val maker: Address,
    val taker: Address?,
    val make: Asset,
    val take: Asset,
    val makePriceUsd: BigDecimal?,
    val takePriceUsd: BigDecimal?,
    val makeUsd: BigDecimal?,
    val takeUsd: BigDecimal?,
    @Id
    val id: ObjectId = ObjectId(),
    val createdAt: Instant = nowMillis(),
    val platform: Platform = Platform.RARIBLE,
    // TODO: Default values here are needed only before the 1st migration ChangeLog00011AddAllFieldsFromOrderToOrderVersion is run
    // to read the old OrderVersions from the database. After that we should remove the default values.
    val type: OrderType = OrderType.RARIBLE_V2,
    val fill: EthUInt256 = EthUInt256.ZERO,
    val makeStock: EthUInt256 = EthUInt256.ZERO,
    val salt: EthUInt256 = EthUInt256.ZERO,
    val start: Long? = null,
    val end: Long? = null,
    val data: OrderData = OrderRaribleV2DataV1(emptyList(), emptyList()),
    val signature: Binary? = null
) {
    fun isBid(): Boolean = take.type.nft

    fun withOrderUsdValue(usdValue: OrderUsdValue): OrderVersion {
        return copy(
            makeUsd = usdValue.makeUsd,
            takeUsd = usdValue.takeUsd,
            makePriceUsd = usdValue.makePriceUsd,
            takePriceUsd = usdValue.takePriceUsd
        )
    }
}

