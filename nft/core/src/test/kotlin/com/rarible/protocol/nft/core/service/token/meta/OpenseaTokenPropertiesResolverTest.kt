package com.rarible.protocol.nft.core.service.token.meta

import com.fasterxml.jackson.databind.ObjectMapper
import com.rarible.core.meta.resource.http.DefaultWebClientBuilder
import com.rarible.core.meta.resource.http.ExternalHttpClient
import com.rarible.core.meta.resource.http.PropertiesHttpLoader
import com.rarible.core.meta.resource.http.ProxyWebClientBuilder
import com.rarible.protocol.nft.core.integration.AbstractIntegrationTest
import com.rarible.protocol.nft.core.integration.IntegrationTest
import com.rarible.protocol.nft.core.model.Token
import com.rarible.protocol.nft.core.model.TokenProperties
import com.rarible.protocol.nft.core.model.TokenStandard
import com.rarible.protocol.nft.core.service.item.meta.BasePropertiesResolverTest.Companion.REQUEST_TIMEOUT
import com.rarible.protocol.nft.core.service.token.meta.descriptors.OpenseaTokenPropertiesResolver
import io.mockk.InternalPlatformDsl.toStr
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import scalether.domain.Address
import scalether.domain.AddressFactory

@IntegrationTest
class OpenseaTokenPropertiesResolverTest : AbstractIntegrationTest() {

    private lateinit var resolver: OpenseaTokenPropertiesResolver
    private lateinit var token: Token

    val defaultWebClientBuilder = DefaultWebClientBuilder(followRedirect = false)
    val proxyWebClientBuilder = ProxyWebClientBuilder(
        readTimeout = 60000,
        connectTimeout = 60000,
        proxyUrl = "",
        followRedirect = false
    )

    @BeforeEach
    fun before() = runBlocking<Unit> {
        token = createToken().copy(
            id = AddressFactory.create(),
            standard = TokenStandard.ERC721
        )
        tokenRepository.save(token).awaitSingle()
    }

    @Test
    fun `should parse from json`() = runBlocking<Unit> {
        val openSeaClient = object : ExternalHttpClient(
            openseaUrl = "https://api.opensea.io/api/v1",
            openseaApiKey = "",
            proxyUrl = "",
            proxyWebClientBuilder = proxyWebClientBuilder,
            defaultWebClientBuilder = defaultWebClientBuilder
        ) {
            override val proxyClient get() = mockOpenSeaResponse("opensea.json")
        }
        resolver = OpenseaTokenPropertiesResolver(
            PropertiesHttpLoader(
                externalHttpClient = openSeaClient,
                defaultRequestTimeout = REQUEST_TIMEOUT,
                openseaRequestTimeout = REQUEST_TIMEOUT
            )
        )
        val props = resolver.resolve(token.id)
        assertThat(props).isEqualTo(
            TokenProperties(
                name = "Feudalz",
                description = "Feudalz emerged to protect their Peasants. When the system run smoothly, it lead to peace and prosperity for everyone.",
                externalLink = "https://feudalz.io",
                image = "https://lh3.googleusercontent.com/wveucmeXBJfqyGiPZDhC1jVaJcx9SH0l2fiLmp2OdLD0KYpFzUIQD_9tTOV57cCDjJ4EjZT6X-Zoyym9eXXHTDxmVfCYzhC_RgkAU0A=s120",
                feeRecipient = Address.apply("0xc00f4b8022e4dc7f086d703328247cb6adf26858"),
                sellerFeeBasisPoints = 250
            )
        )
    }

    @Test
    fun `should parse with unidentified contract name`() = runBlocking<Unit> {
        val openSeaClient = object : ExternalHttpClient(
            openseaUrl = "https://api.opensea.io/api/v1",
            openseaApiKey = "",
            proxyUrl = "",
            proxyWebClientBuilder = proxyWebClientBuilder,
            defaultWebClientBuilder = defaultWebClientBuilder
        ) {
            override val proxyClient get() = mockOpenSeaResponse("opensea_unidentified_contract.json")
        }
        resolver = OpenseaTokenPropertiesResolver(
            PropertiesHttpLoader(
                externalHttpClient = openSeaClient,
                defaultRequestTimeout = REQUEST_TIMEOUT,
                openseaRequestTimeout = REQUEST_TIMEOUT
            )
        )
        val props = resolver.resolve(token.id)
        assertThat(props).isEqualTo(
            TokenProperties(
                name = "My contract",
                description = null,
                externalLink = null,
                image = null,
                feeRecipient = null,
                sellerFeeBasisPoints = null
            )
        )
    }

    private fun mockOpenSeaResponse(resourceName: String): WebClient {
        return WebClient.builder()
            .exchangeFunction { request ->
                assertThat(request.url().toStr()).startsWith("https://api.opensea.io/api/v1/asset_contract/0x")
                Mono.just(
                    ClientResponse.create(HttpStatus.OK)
                        .header("content-type", "application/json")
                        .body(resourceName.asResource())
                        .build()
                )
            }.build()
    }

    fun String.asResource() = this.javaClass::class.java.getResource("/meta/response/$this").readText()
}
