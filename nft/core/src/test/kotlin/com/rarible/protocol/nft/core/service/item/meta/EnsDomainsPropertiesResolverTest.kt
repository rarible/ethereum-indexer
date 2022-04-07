package com.rarible.protocol.nft.core.service.item.meta

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.nft.core.model.ItemAttribute
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.ItemProperties
import com.rarible.protocol.nft.core.service.EnsDomainService
import com.rarible.protocol.nft.core.service.item.meta.descriptors.EnsDomainsPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.descriptors.EnsDomainsPropertiesResolver.Companion.PROPERTIES_NOT_FOUND
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import scalether.domain.Address

@ItemMetaTest
class EnsDomainsPropertiesResolverTest : BasePropertiesResolverTest() {

    private val ensDomainsAddress: Address = Address.apply("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85")
    private val ensDomainService = mockk<EnsDomainService> {
        coEvery { onGetProperties(any(), any()) } returns Unit
    }
    private val resolver = EnsDomainsPropertiesResolver(
        externalHttpClient = ExternalHttpClient(
            openseaUrl = "",
            openseaApiKey = "",
            readTimeout = 10000,
            connectTimeout = 10000,
            proxyUrl = ""
        ),
        ensDomainService = ensDomainService,
        nftIndexerProperties = mockk {
            every { ensDomainsContractAddress } returns ensDomainsAddress.prefixed()
        },
    )

    @Test
    fun `ensDomains resolver - happy path`() = runBlocking<Unit> {
        val properties = resolver.resolve(
            ItemId(
                ensDomainsAddress,
                EthUInt256.of("70978452926855298230627852209706669601671060584535678453189230628746785569329")
            )
        )

        assertThat(properties).isNotNull()
        properties as ItemProperties
        assertThat(properties.name).isEqualTo("rarible.eth")
        assertThat(properties.description).isEqualTo("rarible.eth, an ENS name.")
        assertThat(properties.image).isEqualTo("https://metadata.ens.domains/mainnet/0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85/0x9cec6175a02d670ee2b050842d150cf4233f9755111f9110836ea0305319ba31/image")
        assertThat(properties.imagePreview).isNull()
        assertThat(properties.imageBig).isNull()
        assertThat(properties.animationUrl).isNull()
        assertThat(properties.attributes).containsExactlyInAnyOrder(
            ItemAttribute("Created Date", null),
            ItemAttribute("Length", "7"),
            ItemAttribute("Registration Date", "2020-02-05T21:32:36Z", "string", "date-time"),
            ItemAttribute("Expiration Date", "2030-11-12T12:15:41Z", "string", "date-time"),
        )
        assertThat(properties.rawJsonContent).isEqualTo("{\"is_normalized\":true,\"name\":\"rarible.eth\",\"description\":\"rarible.eth, an ENS name.\",\"attributes\":[{\"trait_type\":\"Created Date\",\"display_type\":\"date\",\"value\":null},{\"trait_type\":\"Length\",\"display_type\":\"number\",\"value\":7},{\"trait_type\":\"Registration Date\",\"display_type\":\"date\",\"value\":1580938356000},{\"trait_type\":\"Expiration Date\",\"display_type\":\"date\",\"value\":1920716141000}],\"name_length\":7,\"url\":\"https://app.ens.domains/name/rarible.eth\",\"version\":0,\"background_image\":\"https://metadata.ens.domains/mainnet/avatar/rarible.eth\",\"image_url\":\"https://metadata.ens.domains/mainnet/0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85/0x9cec6175a02d670ee2b050842d150cf4233f9755111f9110836ea0305319ba31/image\"}")
    }

    @Test
    fun `ensDomains resolver - 404`() = runBlocking<Unit> {
        val properties = resolver.resolve(
            ItemId(
                ensDomainsAddress,
                EthUInt256.of("42")
            )
        )

        assertThat(properties).isEqualTo(PROPERTIES_NOT_FOUND)
    }
}
