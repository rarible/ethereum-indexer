package com.rarible.protocol.nft.core.service.item.reduce.forward

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.Item
import com.rarible.protocol.nft.core.model.ItemEvent
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.service.item.reduce.ReduceContext
import org.springframework.stereotype.Component
import scalether.domain.Address
import kotlin.coroutines.coroutineContext

@Component
class ForwardOpenSeaLazyValueItemReducer(
    nftIndexerProperties: NftIndexerProperties
) : AbstractOpenSeaLazyValueItemReducer(Address.apply(nftIndexerProperties.openseaLazyMintAddress)) {

    override suspend fun reduceItemTransferEvent(entity: Item, event: ItemEvent.ItemTransferEvent): Item {
        val skipOwnerships = coroutineContext[ReduceContext]?.skipOwnerships ?: false

        val creator = getTokenCreator(entity.tokenId)
        return if (creator == event.from) {
            val ownerships = entity.ownerships.toMutableMap()

            if (skipOwnerships.not()) {
                val ownerValue = ownerships[event.from] ?: EthUInt256.ZERO

                //If creator has this owner item on balance, we should compensate decrease on ForwardOwnersItemReducer
                if (event.value > EthUInt256.ZERO && ownerValue > EthUInt256.ZERO) {
                    ownerships[event.from] = ownerValue + event.value
                }
            }
            entity.copy(
                supply = entity.supply + event.value,
                creators = listOf(Part.fullPart(creator)),
                ownerships = ownerships
            )
        } else {
            entity
        }
    }
}
