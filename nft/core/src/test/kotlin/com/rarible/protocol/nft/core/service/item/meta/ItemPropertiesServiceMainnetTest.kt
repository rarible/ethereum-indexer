@file:Suppress("SpellCheckingInspection")

package com.rarible.protocol.nft.core.service.item.meta

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.rarible.ethereum.domain.Blockchain
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.protocol.dto.NftItemMetaDto
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.ItemAttribute
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.ItemProperties
import com.rarible.protocol.nft.core.model.Token
import com.rarible.protocol.nft.core.model.TokenStandard
import com.rarible.protocol.nft.core.service.EnsDomainService
import com.rarible.protocol.nft.core.service.item.meta.descriptors.EnsDomainsPropertiesProvider
import com.rarible.protocol.nft.core.service.item.meta.descriptors.EnsDomainsPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.descriptors.HashmasksPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.descriptors.MutantsBoredApeYachtClubPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.descriptors.OpenSeaPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.descriptors.RariblePropertiesResolver
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import reactor.core.publisher.Mono
import scalether.domain.Address
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.io.path.toPath

@ItemMetaTest
@EnabledIfSystemProperty(named = "RARIBLE_TESTS_OPENSEA_PROXY_URL", matches = ".+")
class ItemPropertiesServiceMainnetTest : BasePropertiesResolverTest() {
    private val rariblePropertiesResolver = RariblePropertiesResolver(
        ipfsService = ipfsService,
        propertiesHttpLoader = propertiesHttpLoader,
        tokenUriResolver = tokenUriResolver
    )

    private val hashmasksPropertiesResolver = HashmasksPropertiesResolver(sender, ipfsService)
    private val mutantsBoredApeYachtClubPropertiesResolver =
        MutantsBoredApeYachtClubPropertiesResolver(externalHttpClient)

    private val openSeaPropertiesResolver = OpenSeaPropertiesResolver(
        externalHttpClient = externalHttpClient,
        requestTimeout = REQUEST_TIMEOUT,
        properties = mockk { every { blockchain } returns Blockchain.ETHEREUM }
    )

    private val nftIndexerProperties = mockk<NftIndexerProperties> {
        every { ensDomainsContractAddress } returns "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
    }

    private val ensDomainsPropertiesProvider = EnsDomainsPropertiesProvider(
        externalHttpClient = ExternalHttpClient(
            openseaUrl = "",
            openseaApiKey = "",
            readTimeout = 10000,
            connectTimeout = 10000,
            proxyUrl = ""
        ),
        nftIndexerProperties = nftIndexerProperties
    )

    private val ensDomainService = mockk<EnsDomainService> {
        coEvery { onGetProperties(any(), any()) } returns Unit
    }

    private val ensResolver = EnsDomainsPropertiesResolver(
        ensDomainService = ensDomainService,
        ensDomainsPropertiesProvider = ensDomainsPropertiesProvider,
        nftIndexerProperties = nftIndexerProperties
    )

    private val service = ItemPropertiesService(
        itemPropertiesResolverProvider = mockk {
            every { orderedResolvers } returns listOf(
                ensResolver,
                hashmasksPropertiesResolver,
                mutantsBoredApeYachtClubPropertiesResolver,
                rariblePropertiesResolver
            )
            every { openSeaResolver } returns openSeaPropertiesResolver
        },
        ipfsService = ipfsService,
        nftIndexerProperties = nftIndexerProperties
    )

    private val jacksonObjectMapper = jacksonObjectMapper()

    @BeforeEach
    fun mockStandard() {
        every { tokenRepository.findById(any()) } answers {
            @Suppress("ReactiveStreamsUnusedPublisher")
            Mono.just(
                Token(
                    firstArg(),
                    name = "",
                    standard = TokenStandard.ERC721
                )
            )
        }
    }

    @Test
    fun `compare rarible and opensea resolvers`() = runBlocking<Unit> {
        val raribleWebUrlPrefix = "https://rarible.com"
        val openSeaWebUrlPrefix = "https://opensea.io/assets"
        val items = listOf(
            // https://rarible.com/token/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d:9187?tab=details
            ItemId(
                Address.apply("0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"),
                EthUInt256.of(9187)
            )
        )
        for (itemId in items) {
            mockTokenStandard(itemId.token, TokenStandard.ERC721)
            println("Comparing ${itemId.decimalStringValue}")
            println("  Rarible Web ${raribleWebUrlPrefix}/${itemId.token}:${itemId.tokenId.value}")
            println("  OpenSea Web ${openSeaWebUrlPrefix}/${itemId.token}/${itemId.tokenId.value}")
            val raribleProperties = rariblePropertiesResolver.resolve(itemId)
            val openSeaProperties = openSeaPropertiesResolver.resolve(itemId)
            println("  Rarible: $raribleProperties")
            println("  OpenSea: $openSeaProperties")
            val resolved = service.resolve(itemId)
            println("  Resolved: $resolved")
        }
    }

    @Test
    fun `exclude date enrichment for ENS domaind`() = runBlocking<Unit> {
        val mockOpenSeaPropertiesResolver = mockk<OpenSeaPropertiesResolver>()

        val service = ItemPropertiesService(
            itemPropertiesResolverProvider = mockk {
                every { orderedResolvers } returns listOf(
                    ensResolver,
                    hashmasksPropertiesResolver,
                    mutantsBoredApeYachtClubPropertiesResolver,
                    rariblePropertiesResolver
                )
                every { openSeaResolver } returns mockOpenSeaPropertiesResolver
            },
            ipfsService = ipfsService,
            nftIndexerProperties = nftIndexerProperties
        )

        val itemId = ItemId(
            Address.apply("0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"),
            EthUInt256.of("107194931414944644333352398451058490772408784024402694127610911420919327942444")
        )
        mockTokenStandard(itemId.token, TokenStandard.ERC721)
        val ensProperties = ensDomainsPropertiesProvider.get(itemId)
        assertThat(ensProperties).isNotNull

        coEvery { mockOpenSeaPropertiesResolver.resolve(any()) } returns ensProperties?.copy(
            name = "Bad",
            description = "Bad"
        )

        val resolved = service.resolve(itemId)

        assertThat(resolved!!.name).isEqualTo("coffeebags.eth")
        assertThat(resolved.description).isEqualTo("coffeebags.eth, an ENS name.")
    }

    @Test
    fun `request meta of public items`() = runBlocking<Unit> {
        val testDataPath = this::class.java.classLoader.getResource("meta")!!.toURI().toPath()
        Files.list(testDataPath).use { list ->
            for (path in list) {
                val itemId = path.name.removeSuffix(".json").let {
                    ItemId(
                        Address.apply(it.substringBefore("_")),
                        EthUInt256.of(it.substringAfter("_").toBigInteger())
                    )
                }
                println("Processing $itemId")
                val itemProperties = service.resolve(itemId)
                val expectedProperties = parseExpectedProperties(path)
                assertThat(itemProperties).isNotNull; itemProperties!!
                assertThat(itemProperties.withSortedAttributes()).isEqualToIgnoringGivenFields(
                    expectedProperties.withSortedAttributes(),
                    "rawJsonContent"
                )
            }
        }
    }

    private fun ItemProperties.withSortedAttributes() =
        copy(attributes = attributes.sortedBy { it.key })

    private fun parseExpectedProperties(path: Path): ItemProperties {
        val jsonNode = jacksonObjectMapper.readTree(path.readText())
        val expectedMetaDto = jacksonObjectMapper.treeToValue<NftItemMetaDto>(jsonNode)!!
        return expectedMetaDto.itemProperties()
    }

    private fun NftItemMetaDto.itemProperties() = ItemProperties(
        name = name,
        description = description,
        image = image?.url?.get("ORIGINAL"),
        imagePreview = image?.url?.get("PREVIEW"),
        imageBig = image?.url?.get("BIG"),
        animationUrl = animation?.url?.get("ORIGINAL"),
        attributes = attributes.orEmpty().map {
            ItemAttribute(
                key = it.key,
                value = it.value,
                type = it.type,
                format = it.format
            )
        },
        rawJsonContent = null
    )
}
