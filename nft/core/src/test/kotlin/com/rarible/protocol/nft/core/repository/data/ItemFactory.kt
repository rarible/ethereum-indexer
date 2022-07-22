package com.rarible.protocol.nft.core.repository.data

import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.nft.core.model.Item
import com.rarible.protocol.nft.core.model.Part
import scalether.domain.AddressFactory
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom

fun createItem(): Item {
    val token = createAddress()
    val tokenId = EthUInt256.of(ThreadLocalRandom.current().nextLong(1, 10000))
    return Item(
        token = token,
        tokenId = tokenId,
        creators = listOf(Part(AddressFactory.create(), 1000)),
        supply = EthUInt256.of(BigInteger.valueOf(ThreadLocalRandom.current().nextLong(1, 10000))),
        lazySupply = EthUInt256.of(BigInteger.valueOf(ThreadLocalRandom.current().nextLong(1, 10000))),
        royalties = (1..ThreadLocalRandom.current().nextInt(1, 20)).map { createRoyalty() },
        owners = (1..ThreadLocalRandom.current().nextInt(1, 20)).map { createAddress() },
        date = nowMillis(),
        isRaribleContract = false
    )
}
