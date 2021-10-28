package com.rarible.protocol.nft.core.service.item.meta

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.core.common.nowMillis
import com.rarible.ethereum.domain.Blockchain
import com.rarible.protocol.nft.core.configuration.IpfsProperties
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.*
import com.rarible.protocol.nft.core.repository.TemporaryItemPropertiesRepository
import com.rarible.protocol.nft.core.repository.TokenRepository
import com.rarible.protocol.nft.core.repository.history.LazyNftItemHistoryRepository
import com.rarible.protocol.nft.core.service.item.meta.descriptors.*
import io.daonomic.rpc.mono.WebClientTransport
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.transaction.ReadOnlyMonoTransactionSender
import java.math.BigInteger
import java.time.temporal.ChronoUnit

@Tag("manual")
@Disabled
class ItemPropertiesServiceTest {
    private val mapper = ObjectMapper().registerKotlinModule()
    private val tokenRepository = mockk<TokenRepository>()
    private val lazyNftItemHistoryRepository = mockk<LazyNftItemHistoryRepository>()
    private val temporaryItemPropertiesRepository = mockk<TemporaryItemPropertiesRepository>()
    private val sender = ReadOnlyMonoTransactionSender(MonoEthereum(WebClientTransport("https://dark-solitary-resonance.quiknode.pro/c0b7c629520de6c3d39261f6417084d71c3f8791/", MonoEthereum.mapper(), 10000, 10000)), Address.ZERO())
    private val ipfsProps = IpfsProperties("https://pinata.rarible.com/upload", "https://rarible.mypinata.cloud/ipfs")
    private val ipfsService = IpfsService(IpfsService.IPFS_NEW_URL, ipfsProps)
    private val propertiesCacheDescriptor = PropertiesCacheDescriptor(sender, tokenRepository, lazyNftItemHistoryRepository, ipfsService, 86400, 20000)
    private val kittiesCacheDescriptor = KittiesCacheDescriptor(86400)
    private val lootCacheDescriptor = LootCacheDescriptor(86400, sender, mapper, ipfsService)
    private val properties = NftIndexerProperties("", Blockchain.ETHEREUM, "0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB", "", "", mockk())
    private val yInsureCacheDescriptor = YInsureCacheDescriptor(sender, "0x181aea6936b407514ebfc0754a37704eb8d98f91", 86400, "0x1776651F58a17a50098d31ba3C3cD259C1903f7A", "http://localhost:8080")
    private val hegicCacheDescriptor = HegicCacheDescriptor(sender, "0xcb9ebae59738d9dadc423adbde66c018777455a4", 86400, "http://localhost:8080")
    private val hashmasksCacheDescriptor = HashmasksCacheDescriptor(sender, "0xc2c747e0f7004f9e8817db2ca4997657a7746928", 86400)
    private val waifusionCacheDescriptor = WaifusionCacheDescriptor(sender, "0x2216d47494e516d8206b70fca8585820ed3c4946", 86400, "https://ipfs.io/ipfs/QmQuzMGqHxSXugCUvWQjDCDHWhiGB75usKbrZk6Ec6pFvw")
    private val testing = ItemPropertiesService(
        propertiesCacheDescriptor,
        kittiesCacheDescriptor,
        lootCacheDescriptor,
        yInsureCacheDescriptor,
        hegicCacheDescriptor,
        hashmasksCacheDescriptor,
        waifusionCacheDescriptor,
        mockk(),
        OpenSeaCacheDescriptor("https://api.opensea.io/api/v1", "", 10000, 3000, 86400, 20000, "", null),
        ipfsService,
        temporaryItemPropertiesRepository,
        properties,
        "0x181aea6936b407514ebfc0754a37704eb8d98f91",
        "0xcb9ebae59738d9dadc423adbde66c018777455a4",
        "0xc2c747e0f7004f9e8817db2ca4997657a7746928",
        "0x2216d47494e516d8206b70fca8585820ed3c4946",
        null
    )

    @BeforeEach
    private fun before() {
        every { tokenRepository.findById(any()) } returns Mono.empty()
        every { lazyNftItemHistoryRepository.findLazyMintById(any()) } returns Mono.empty()
        every { temporaryItemPropertiesRepository.findById(any<String>()) } returns Mono.empty()
    }

    @Test
    fun cryptoKitties() {
        val props = kittiesCacheDescriptor.get("${ItemPropertiesService.CRYPTO_KITTIES}:1001").block()!!
        assertEquals(props.name, "TheFirst")
        assertEquals(
            "Hey cutie! I'm TheFirst. In high school, I was voted most likely to work at NASA. " +
                    "When my owner isn't watching, I steal their oversized sweaters and use them for litter paper. " +
                    "I'm not sorry. I think you'll love me beclaws I have cattitude.",
            props.description
        )
        assertEquals("https://img.cn.cryptokitties.co/0x06012c8cf97bead5deae237070f9587f8e7a266d/1001.svg", props.image)
        assertEquals(props.attributes[0].key, "colorprimary")
        assertEquals(props.attributes[0].value, "shadowgrey")
    }

    @Test
    fun lootProperties() {
        val props = lootCacheDescriptor.get("${ItemPropertiesService.LOOT_ADDRESS}:10").block()!!
        assertEquals("Bag #10", props.name)
        assertEquals("Loot is randomized adventurer gear generated and stored on chain. Stats, images, and other functionality are intentionally omitted for others to interpret. Feel free to use Loot in any way you want.",
            props.description
        )
        assertEquals(8, props.attributes.size)
        assertThat(props.attributes).contains(ItemAttribute("chest", "Robe"))
        assertThat(props.attributes).contains(ItemAttribute("foot", "Holy Greaves"))
        assertThat(props.attributes).contains(ItemAttribute("ring", "Platinum Ring"))
        assertThat(props.attributes).contains(ItemAttribute("weapon", "Maul"))
        assertThat(props.attributes).contains(ItemAttribute("waist", "Studded Leather Belt"))
        assertThat(props.attributes).contains(ItemAttribute("hand", "Wool Gloves"))
        assertThat(props.attributes).contains(ItemAttribute("head", "Divine Hood"))
        assertThat(props.attributes).contains(ItemAttribute("neck", "\"Havoc Sun\" Amulet of Reflection"))
        assertEquals("ipfs://ipfs/QmaA5x5Hj9gQRaTLbpVzv3ruvSjF2T9FMLkPqtCQzXYJbU", props.image)
        assertEquals("ipfs://ipfs/QmaA5x5Hj9gQRaTLbpVzv3ruvSjF2T9FMLkPqtCQzXYJbU", props.animationUrl)
    }

    @Test
    fun uniswapProperties() {
        val props = propertiesCacheDescriptor.get("0xc36442b4a4522e871399cd717abdd847ab11fe88:51561").block()!!
        assertEquals("Uniswap - 0.3% - MATIC/WETH - 1555.6<>1603.0", props.name)
        assertEquals("""This NFT represents a liquidity position in a Uniswap V3 MATIC-WETH pool. The owner of this NFT can modify or redeem the position.

Pool Address: 0x290a6a7460b308ee3f19023d2d00de604bcf5b42
MATIC Address: 0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0
WETH Address: 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
Fee Tier: 0.3%
Token ID: 51561

⚠️ DISCLAIMER: Due diligence is imperative when assessing this NFT. Make sure token addresses match the expected tokens, as token symbols may be imitated.
""".trim(), props.description?.trim())
        assertEquals("ipfs://ipfs/QmTeoZ678pim8mFdVqrEsPfAaMJnnofH6G7Z4MWjFqoFxx", props.image)
        assertEquals("ipfs://ipfs/QmTeoZ678pim8mFdVqrEsPfAaMJnnofH6G7Z4MWjFqoFxx", props.animationUrl)
    }

    @Test
    fun getCustomProperties() {
        val props = testing.getProperties(Address.apply("0x9b1aa69fe9fca10aa41250bba054aabd92aba5b6"), BigInteger.valueOf(116L)).block()!!
        assertEquals(props.name, "↜Warrior↝ 6/20")
    }

    @Test
    fun erc1155() {
        val address = Address.apply("0xfaafdc07907ff5120a76b34b731b278c38d6043c")
        every { tokenRepository.findById(address) } returns Mono.just(Token(address, name = "", standard = TokenStandard.ERC1155))

        val props = testing.getProperties(address, "10855508365998400056289941914472950957046112164229867356526540410650888241152".toBigInteger()).block()!!
        assertEquals(props.name, "Argent")
        assertEquals(props.description, "Straight forward, no fuss, iron cast stock and woodgrain grips. \"The Argent Defender.\"")
        assertEquals("https://alterverse.sfo2.digitaloceanspaces.com/WalletArt/Disruption/Intimidators/WalletArt_Argent.jpg", props.image)
    }

    @Test
    fun yInsure() {
        val props = testing.getProperties(Address.apply("0x181aea6936b407514ebfc0754a37704eb8d98f91"), "48".toBigInteger()).block()!!
        assertEquals("Balancer | 1 ETH \uD83D\uDD12 | 11/11/2020", props.name)
        assertEquals("http://localhost:8080/image/yinsure/48.svg", props.image)
        assertEquals(11, props.attributes.size)
    }

    @Test
    fun hegic() {
        val props = testing.getProperties(Address.apply("0xcb9ebae59738d9dadc423adbde66c018777455a4"), "317".toBigInteger()).block()!!
        assertTrue(props.name.endsWith("call option for 1 ETH. Expires on 20.11.2020 07h44 UTC"))
        assertEquals("http://localhost:8080/image/hegic/317.svg", props.image)
        assertEquals(13, props.attributes.size)
    }

    @Test
    fun hashmasks() {
        val props = testing.getProperties(Address.apply("0xc2c747e0f7004f9e8817db2ca4997657a7746928"), "9076".toBigInteger()).block()!!
        assertEquals("https://hashmasksstore.blob.core.windows.net/hashmasks/2833.jpg", props.image)
        assertEquals(2, props.attributes.size)
    }

    @Test
    fun waifusion() {
        val props = testing.getProperties(Address.apply("0x2216d47494e516d8206b70fca8585820ed3c4946"), "1".toBigInteger()).block()!!
        assertEquals("Kurisu Makise", props.name)
        assertEquals("Waifusion is a digital Waifu collection. There are 16,384 guaranteed-unique Waifusion NFTs. They’re just like you; a beautiful work of art, but 2-D and therefore, superior, Anon-kun.", props.description)
        assertEquals("ipfs://ipfs/QmQuzMGqHxSXugCUvWQjDCDHWhiGB75usKbrZk6Ec6pFvw/1.png", props.image)
        assertEquals(2, props.attributes.size)
    }

    @Test
    fun standardWithRedirect() {
        val props = testing.getProperties(Address.apply("0xbd13e53255ef917da7557db1b7d2d5c38a2efe24"), 967928015015.toBigInteger()).block()!!
        assertEquals(props.name, "Rio")
        assertEquals(props.description, "Rio is DozerFriends’ adorable yellow bear, happy as sunshine and always thirsty for his favourite drink made of yellow flowers.")
        assertEquals(props.image, "https://cryptodozer.io/static/images/dozer/meta/dolls/100.png")
    }

    @Test
    internal fun boredApeYachtClub() {
        val properties = testing.getProperties(Address.apply("0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"), BigInteger.valueOf(9163)).block()!!
        assertEquals("BoredApeYachtClub #9163", properties.name)
        assertEquals(6, properties.attributes.size)
        assertThat(properties.attributes).contains(ItemAttribute("Fur", "Black"))
        assertThat(properties.attributes).contains(ItemAttribute("Background", "New Punk Blue"))
        assertThat(properties.attributes).contains(ItemAttribute("Clothes", "Biker Vest"))
        assertThat(properties.attributes).contains(ItemAttribute("Eyes", "Bored"))
        assertThat(properties.attributes).contains(ItemAttribute("Hat", "Army Hat"))
        assertThat(properties.attributes).contains(ItemAttribute("Mouth", "Phoneme Vuh"))
    }

    @Test
    internal fun etherCats() {
        val properties = testing.getProperties(Address.apply("0xff3559412c4618af7c6e6f166c74252ff6364456"), BigInteger.valueOf(50101)).block()!!
        assertEquals("Gatinho [10 Votes, Common]", properties.name)
        assertThat(properties.description).contains("Gatinho")
        assertEquals(properties.image, "ipfs://ipfs/QmQSKwVhvTcfpgz8g47XgfvrSHTWe6a29WARdDs2uUHcZE/50101.png")
        assertEquals(9, properties.attributes.size)
        assertThat(properties.attributes).contains(ItemAttribute("Multiplier", "1"))
        assertThat(properties.attributes).contains(ItemAttribute("Rating", "10"))
        assertThat(properties.attributes).contains(ItemAttribute("Total Score", "10"))
        assertThat(properties.attributes).contains(ItemAttribute("Rarity", "Common"))
        assertThat(properties.attributes).contains(ItemAttribute("North", "8"))
        assertThat(properties.attributes).contains(ItemAttribute("West", "1"))
        assertThat(properties.attributes).contains(ItemAttribute("Personality Suit", "Punter"))
        assertThat(properties.attributes).contains(ItemAttribute("East", "Stringalong"))
        assertThat(properties.attributes).contains(ItemAttribute("South", "Box"))
    }

    @Test
    fun goldenStellaWithAnimationUrl() {
        val props = testing.getProperties(Address.apply("0xdb7e971d39367b20bcf4df5ae2da0fa4261bf0e8"), 426.toBigInteger()).block()!!
        assertEquals(props.name, "Golden Stella [Batch 1]")
        assertEquals(props.animationUrl, "https://storage.opensea.io/files/33d17df2d5c0411521a58cd88491de35.glb")
    }

    @Test
    fun russianMountainWithAnimationUrl() {
        val props = testing.getProperties(Address.apply("0xfbeef911dc5821886e1dda71586d90ed28174b7d"), 200026.toBigInteger()).block()!!
        assertEquals(props.name, "A Russian Mountain")
        assertEquals(props.animationUrl, "https://storage.opensea.io/files/8f32a2196bac4572c7b9eb8cba33f27a.mp4")
    }

    @Test
    fun russianMountainWithAnimationUrlViaContract() {
        val itemProperties = propertiesCacheDescriptor.get("0xfbeef911dc5821886e1dda71586d90ed28174b7d:200026").block()!!
        assertEquals(
            itemProperties.animationUrl,
            "https://ipfs.infura.io/ipfs/QmY5c8rW2W4M8qUCiG4RSymprHvMPxhDfhRLUc2u5YMDJN/asset.mp4"
        )
    }

    @Test
    fun keepCalm() {
        val props = testing.getProperties(Address.apply("0x1866c6907e70eb176109363492b95e3617b4a195"), 27.toBigInteger()).block()!!
        assertEquals(props.name, "Freaky Hallway")
    }

    @Test
    fun cryptoPunks() {
        val props = testing.getProperties(Address.apply("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"), 33.toBigInteger()).block()!!
        assertEquals("CryptoPunk #33", props.name)
        assertEquals("https://www.larvalabs.com/cryptopunks/cryptopunk33.png", props.image)
        assertEquals(listOf("accessory" to "Peak Spike", "type" to "Male"), props.attributes.sortedBy { it.key }.map { it.key to it.value })
    }

    @Test
    fun standardImagePreview() {
        val props = testing.getProperties(Address.apply("0xe414a94e31f5f31e640a26d2822e8fef3328b667"), 536.toBigInteger()).block()!!
        assertNotNull(props.imagePreview)
    }

    @Test
    fun immutable() {
        val props = testing.getProperties(Address.apply("0x0e3a2a1f2146d86a604adc220b4967a898d7fe07"), 187594956.toBigInteger()).block()!!
        assertEquals(props.image, "https://api.immutable.com/asset/0x0e3a2a1f2146d86a604adc220b4967a898d7fe07/187594956")
    }

    @Test
    fun standard() {
        val props = testing.getProperties(Address.apply("0xdceaf1652a131F32a821468Dc03A92df0edd86Ea"), 10400663.toBigInteger()).block()!!
        assertEquals(props.name, "MCH Extension: #10400663 Lv.60")
        assertTrue(props.description!!.contains("Extensions"), props.description)
        assertEquals(props.image, "https://www.mycryptoheroes.net/images/extensions/2000/1040.png")
        val attr = props.attributes.find { it.key == "type_name" }
        assertNotNull(attr)
        assertEquals(attr!!.value, "Tangerine")
        assertEquals(props.attributes.size, 9)
    }

    @Test
    fun standard2() {
        val props = testing.getProperties(Address.apply("0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab"), 142708.toBigInteger()).block()!!
        assertEquals(props.name, "Simple Satyr")
        assertEquals("At the end of your turn, heal your god for 2.", props.description!!)
        assertEquals(props.image, "https://images.godsunchained.com/cards/250/43.png")
        assertTrue(props.attributes.any { it.key == "god" && it.value == "nature" })
    }

    @Test
    fun rarible() {
        val props = testing.getProperties(Address.apply("0xf79ab01289f85b970bf33f0543e41409ed2e1c1f"), 16.toBigInteger()).block()!!
        assertTrue(props.description!!.contains("Hi!"))
        assertEquals("ipfs://ipfs/QmVVBP63aBS9oyRpbw8LBhu8oPkTwkMhNwLYLS6yiH7apC", props.image)
        assertEquals(props.attributes[0].key, "First Name")
        assertEquals(props.attributes[0].value?.toLowerCase(), "alex")
    }

    @Test
    fun ens() {
        val props = testing.getProperties(Address.apply("0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"), "36071955485891595785443576745556172890537718974602336938223322572747980843639".toBigInteger()).block()!!
        assertTrue(props.name.contains(".eth"))
    }

    @Test
    fun fromTemporaryItemPropertiesRepository() {
        val token = Address.THREE()
        val tokenId = 2.toBigInteger()
        val id = "$token:$tokenId"
        val name = "name_testing"
        val temporaryItemProperties = TemporaryItemProperties(
            id = id,
            value = ItemProperties(
                name = name,
                description = "description",
                image = "image",
                imagePreview = "imagePreview",
                imageBig = "imageBig",
                animationUrl = "animationUrl",
                attributes = listOf()
            ),
            createDate = nowMillis().minus(1, ChronoUnit.DAYS)
        )
        every { temporaryItemPropertiesRepository.findById(id) } returns Mono.just(temporaryItemProperties)

        val props = testing.getProperties(token, tokenId).block()
        assertNotNull(props)
        assertEquals(props!!.name, name)
    }

    @Test
    fun testDatetimeFromOpensea() {
        val descriptor = OpenSeaCacheDescriptor("https://rinkeby-api.opensea.io/api/v1", "", 10000, 3000, 86400, 20000, "", null)
        val meta = descriptor.get("0x20d34e12657b2317be1ef1384d9d54cef087dcae:26").block()!!
        assertThat(meta.attributes).contains(ItemAttribute("Active Since", "2020-12-08T00:00:00Z", "string", "date-time"))
        assertEquals(5, meta.attributes.size)
    }
}
