package com.rarible.protocol.nft.core.integration

import com.rarible.core.application.ApplicationEnvironmentInfo
import com.rarible.core.daemon.sequential.ConsumerWorker
import com.rarible.protocol.dto.NftItemEventDto
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.ReduceSkipTokens
import com.rarible.protocol.nft.core.service.item.meta.InternalItemHandler
import com.rarible.protocol.nft.core.service.item.meta.ItemPropertiesResolver
import com.rarible.protocol.nft.core.service.item.meta.ItemPropertiesResolverProvider
import io.daonomic.rpc.mono.WebClientTransport
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.core.PubSubTransport
import scalether.domain.Address
import scalether.transaction.MonoTransactionPoller
import scalether.transaction.ReadOnlyMonoTransactionSender
import scalether.transport.WebSocketPubSubTransport

@TestConfiguration
class TestPropertiesConfiguration {
    @Bean
    fun skipTokens(): ReduceSkipTokens {
        return ReduceSkipTokens(hashSetOf())
    }

    @Bean
    fun testEthereum(@Value("\${parityUrls}") url: String): MonoEthereum {
        return MonoEthereum(WebClientTransport(url, MonoEthereum.mapper(), 10000, 10000))
    }

    @Bean
    fun testSender(ethereum: MonoEthereum) = ReadOnlyMonoTransactionSender(ethereum, Address.ONE())

    @Bean
    fun poller(ethereum: MonoEthereum): MonoTransactionPoller {
        return MonoTransactionPoller(ethereum)
    }

    @Bean
    fun pubSubTransport(@Value("\${parityUrls}") url: String): WebSocketPubSubTransport {
        return WebSocketPubSubTransport(url, Int.MAX_VALUE)
    }

    @Bean
    fun ethPubSub(transport: PubSubTransport): EthPubSub {
        return EthPubSub(transport)
    }

    @Bean
    fun applicationEnvironmentInfo(): ApplicationEnvironmentInfo {
        return ApplicationEnvironmentInfo("localhost", "e2e")
    }

    @Bean
    fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()

    @Bean
    fun itemMetaExtenderWorker() {

    }

    @Bean
    @Primary
    @Qualifier("mockItemPropertiesResolver")
    fun mockItemPropertiesResolver(): ItemPropertiesResolver = mockk {
        every { name } returns "MockResolver"
        every { canBeCached } returns true
    }

    @Bean
    @Primary
    fun mockItemPropertiesResolverProvider(
        @Qualifier("mockItemPropertiesResolver") mockItemPropertiesResolver: ItemPropertiesResolver
    ): ItemPropertiesResolverProvider = mockk {
        every { orderedResolvers } returns listOf(mockItemPropertiesResolver)
    }

    /**
     * This bean is needed to make possible publishing of item with extended meta.
     * In production this bean is defined in the 'nft-indexer-listener' module.
     */
    @Bean
    fun itemMetaExtenderWorker(
        applicationEnvironmentInfo: ApplicationEnvironmentInfo,
        internalItemHandler: InternalItemHandler,
        nftIndexerProperties: NftIndexerProperties,
        meterRegistry: MeterRegistry
    ): ConsumerWorker<NftItemEventDto> {
        return ConsumerWorker(
            consumer = InternalItemHandler.createInternalItemConsumer(
                applicationEnvironmentInfo,
                nftIndexerProperties.blockchain,
                nftIndexerProperties.kafkaReplicaSet
            ),
            properties = nftIndexerProperties.daemonWorkerProperties,
            eventHandler = internalItemHandler,
            meterRegistry = meterRegistry,
            workerName = "nftItemMetaExtender"
        )
    }

    @Bean
    fun itemMetaExtenderWorkerStarter(itemMetaExtenderWorker: ConsumerWorker<NftItemEventDto>): CommandLineRunner =
        CommandLineRunner { itemMetaExtenderWorker.start() }
}
