package com.rarible.protocol.nft.core.converters.dto

import com.rarible.protocol.dto.NftCollectionDto
import com.rarible.protocol.nft.core.model.ContractStatus
import com.rarible.protocol.nft.core.model.ExtendedToken
import com.rarible.protocol.nft.core.model.TokenFeature
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
class ExtendedCollectionDtoConverter : Converter<ExtendedToken, NftCollectionDto> {
    override fun convert(source: ExtendedToken): NftCollectionDto {
        val (token, meta) = source
        return NftCollectionDto(
            id = token.id,
            type = CollectionTypeDtoConverter.convert(token.standard),
            status = convertStatus(token.status),
            owner = token.owner,
            name = token.name,
            symbol = token.symbol,
            features = token.features.map { CollectionFeatureDtoConverter.convert(it) },
            supportsLazyMint = token.features.contains(TokenFeature.MINT_AND_TRANSFER),
            minters = if (token.isRaribleContract) listOfNotNull(token.owner) else emptyList(),
            meta = NftCollectionMetaDtoConverter.convert(meta),
            isRaribleContract = token.isRaribleContract
        )
    }

    private fun convertStatus(tokenStatus: ContractStatus): NftCollectionDto.Status =
        when (tokenStatus) {
            ContractStatus.PENDING -> NftCollectionDto.Status.PENDING
            ContractStatus.ERROR -> NftCollectionDto.Status.ERROR
            ContractStatus.CONFIRMED -> NftCollectionDto.Status.CONFIRMED
        }
}
