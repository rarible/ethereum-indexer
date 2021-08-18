package com.rarible.protocol.nftorder.listener.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.core.application.ApplicationEnvironmentInfo
import com.rarible.core.daemon.sequential.ConsumerWorker
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.json.JsonDeserializer
import com.rarible.core.kafka.json.JsonSerializer
import com.rarible.core.task.EnableRaribleTask
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import com.rarible.ethereum.domain.Blockchain
import com.rarible.protocol.dto.*
import com.rarible.protocol.nft.api.subscriber.NftIndexerEventsConsumerFactory
import com.rarible.protocol.nftorder.listener.handler.ItemEventHandler
import com.rarible.protocol.nftorder.listener.handler.OrderEventHandler
import com.rarible.protocol.nftorder.listener.handler.OwnershipEventHandler
import com.rarible.protocol.nftorder.listener.handler.UnlockableEventHandler
import com.rarible.protocol.order.api.subscriber.OrderIndexerEventsConsumerFactory
import com.rarible.protocol.order.api.subscriber.autoconfigure.OrderIndexerEventsSubscriberProperties
import com.rarible.protocol.unlockable.api.subscriber.UnlockableEventsConsumerFactory
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
@EnableMongock
@EnableRaribleTask
@EnableScaletherMongoConversions
@EnableConfigurationProperties(
    value = [
        NftOrderListenerProperties::class,
        NftOrderEventProducerProperties::class,
        NftOrderJobProperties::class
    ]
)
class NftOrderListenerConfiguration(
    private val environmentInfo: ApplicationEnvironmentInfo,
    private val listenerProperties: NftOrderListenerProperties,
    private val eventProducerProperties: NftOrderEventProducerProperties,
    private val meterRegistry: MeterRegistry,
    private val blockchain: Blockchain,
    private val orderIndexerSubscriberProperties: OrderIndexerEventsSubscriberProperties
) {
    private val itemConsumerGroup = "${environmentInfo.name}.protocol.${blockchain.value}.nft-order.item"
    private val ownershipConsumerGroup = "${environmentInfo.name}.protocol.${blockchain.value}.nft-order.ownership"
    private val unlockableConsumerGroup = "${environmentInfo.name}.protocol.${blockchain.value}.nft-order.unlockable"
    private val orderConsumerGroup = "${environmentInfo.name}.protocol.${blockchain.value}.nft-order.order"

    @Bean
    fun itemChangeWorker(
        nftIndexerEventsConsumerFactory: NftIndexerEventsConsumerFactory,
        itemEventHandler: ItemEventHandler
    ): ConsumerWorker<NftItemEventDto> {
        return ConsumerWorker(
            consumer = nftIndexerEventsConsumerFactory.createItemEventsConsumer(itemConsumerGroup),
            properties = listenerProperties.monitoringWorker,
            eventHandler = itemEventHandler,
            meterRegistry = meterRegistry,
            workerName = "itemEventDto"
        )
    }

    @Bean
    fun ownershipChangeWorker(
        nftIndexerEventsConsumerFactory: NftIndexerEventsConsumerFactory,
        ownershipEventHandler: OwnershipEventHandler
    ): ConsumerWorker<NftOwnershipEventDto> {
        return ConsumerWorker(
            consumer = nftIndexerEventsConsumerFactory.createOwnershipEventsConsumer(ownershipConsumerGroup),
            properties = listenerProperties.monitoringWorker,
            eventHandler = ownershipEventHandler,
            meterRegistry = meterRegistry,
            workerName = "ownershipEventDto"
        )
    }

    @Bean
    fun unlockableChangeWorker(
        unlockableEventsConsumerFactory: UnlockableEventsConsumerFactory,
        unlockableEventHandler: UnlockableEventHandler
    ): ConsumerWorker<UnlockableEventDto> {
        return ConsumerWorker(
            consumer = unlockableEventsConsumerFactory.createUnlockableEventsConsumer(unlockableConsumerGroup),
            properties = listenerProperties.monitoringWorker,
            eventHandler = unlockableEventHandler,
            meterRegistry = meterRegistry,
            workerName = "unlockableEventDto"
        )
    }

    @Bean
    fun orderChangeWorker(
        orderIndexerEventsConsumerFactory: OrderIndexerEventsConsumerFactory,
        orderEventHandler: OrderEventHandler
    ): ConsumerWorker<OrderEventDto> {
        return ConsumerWorker(
            consumer = createOrderEventsConsumer(orderConsumerGroup),
            properties = listenerProperties.monitoringWorker,
            eventHandler = orderEventHandler,
            meterRegistry = meterRegistry,
            workerName = "orderEventDto"
        )
    }

    @Bean
    fun itemEventProducer(): RaribleKafkaProducer<NftOrderItemEventDto> {
        val env = eventProducerProperties.environment
        val blockchain = blockchain.value

        val clientId = "${env}.${blockchain}.protocol-unlockable.lock"

        return RaribleKafkaProducer(
            clientId = clientId,
            valueSerializerClass = JsonSerializer::class.java,
            valueClass = NftOrderItemEventDto::class.java,
            defaultTopic = NftOrderItemEventTopicProvider.getTopic(env, blockchain),
            bootstrapServers = eventProducerProperties.kafkaReplicaSet
        )
    }

    @Bean
    fun ownershipEventProducer(): RaribleKafkaProducer<NftOrderOwnershipEventDto> {
        val env = eventProducerProperties.environment
        val blockchain = blockchain.value

        val clientId = "${env}.${blockchain}.protocol-unlockable.lock"

        return RaribleKafkaProducer(
            clientId = clientId,
            valueSerializerClass = JsonSerializer::class.java,
            valueClass = NftOrderOwnershipEventDto::class.java,
            defaultTopic = NftOrderOwnershipEventTopicProvider.getTopic(env, blockchain),
            bootstrapServers = eventProducerProperties.kafkaReplicaSet
        )
    }

    // TODO remove when order-subscriber start to use .global topic by default
    fun createOrderEventsConsumer(consumerGroup: String): RaribleKafkaConsumer<OrderEventDto> {
        val clientIdPrefix = "${environmentInfo.name}.${blockchain.value}.${environmentInfo.name}.${UUID.randomUUID()}"
        return RaribleKafkaConsumer(
            clientId = "$clientIdPrefix.order-indexer-order-events-consumer",
            valueDeserializerClass = JsonDeserializer::class.java,
            valueClass = OrderEventDto::class.java,
            consumerGroup = consumerGroup,
            defaultTopic = OrderIndexerTopicProvider.getTopic(environmentInfo.name, blockchain.value) + ".global",
            bootstrapServers = orderIndexerSubscriberProperties.brokerReplicaSet
        )
    }

}