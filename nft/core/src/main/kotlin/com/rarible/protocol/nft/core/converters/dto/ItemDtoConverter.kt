package com.rarible.protocol.nft.core.converters.dto

import com.rarible.protocol.dto.NftItemDto
import com.rarible.protocol.nft.core.model.Item
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
class ItemDtoConverter : Converter<Item, NftItemDto> {
    override fun convert(item: Item): NftItemDto =
        NftItemDto(
            id = item.id.decimalStringValue,
            contract = item.token,
            tokenId = item.tokenId.value,
            creators = item.creators.map { PartDtoConverter.convert(it) },
            supply = item.supply.value,
            lazySupply = item.lazySupply.value,
            lastUpdatedAt = item.date,
            mintedAt = item.mintedAt,
            deleted = item.deleted,
            isRaribleContract = item.isRaribleContract
        )
}