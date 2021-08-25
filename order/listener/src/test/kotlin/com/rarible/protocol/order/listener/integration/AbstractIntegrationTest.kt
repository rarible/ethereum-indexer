package com.rarible.protocol.order.listener.integration

import com.rarible.core.application.ApplicationEnvironmentInfo
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.json.JsonDeserializer
import com.rarible.core.test.ext.KafkaTestExtension.Companion.kafkaContainer
import com.rarible.core.test.wait.Wait
import com.rarible.ethereum.listener.log.domain.LogEvent
import com.rarible.ethereum.listener.log.domain.LogEventStatus
import com.rarible.protocol.dto.*
import com.rarible.protocol.order.core.configuration.OrderIndexerProperties
import com.rarible.protocol.order.core.model.Order
import com.rarible.protocol.order.core.model.OrderExchangeHistory
import com.rarible.protocol.order.core.repository.exchange.ExchangeHistoryRepository
import com.rarible.protocol.order.core.repository.order.OrderRepository
import com.rarible.protocol.order.core.service.OrderReduceService
import com.rarible.protocol.order.core.service.OrderVersionService
import io.daonomic.rpc.domain.Request
import io.daonomic.rpc.domain.Word
import io.daonomic.rpc.domain.WordFactory
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.awaitFirst
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.core.MonoParity
import scalether.domain.Address
import scalether.domain.AddressFactory
import scalether.domain.response.TransactionReceipt
import scalether.java.Lists
import scalether.transaction.*
import java.math.BigInteger
import java.util.*
import javax.annotation.PostConstruct

@FlowPreview
abstract class AbstractIntegrationTest : BaseListenerApplicationTest() {
    private val logger = LoggerFactory.getLogger(javaClass)
    protected lateinit var sender: MonoTransactionSender

    @Autowired
    private lateinit var application: ApplicationEnvironmentInfo

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    @Autowired
    protected lateinit var exchangeHistoryRepository: ExchangeHistoryRepository

    @Autowired
    protected lateinit var orderRepository: OrderRepository

    @Autowired
    protected lateinit var orderIndexerProperties: OrderIndexerProperties

    @Autowired
    protected lateinit var orderReduceService: OrderReduceService

    @Autowired
    protected lateinit var orderVersionService: OrderVersionService

    @Autowired
    protected lateinit var ethereum: MonoEthereum

    @Autowired
    protected lateinit var poller: MonoTransactionPoller

    protected lateinit var parity: MonoParity

    protected lateinit var consumer: RaribleKafkaConsumer<ActivityDto>

    private fun Mono<Word>.waitReceipt(): TransactionReceipt {
        val value = this.block()
        require(value != null) { "txHash is null" }
        return ethereum.ethGetTransactionReceipt(value).block()!!.get()
    }

    @BeforeEach
    fun cleanDatabase() {
        mongo.collectionNames
            .filter { !it.startsWith("system") }
            .flatMap { mongo.remove(Query(), it) }
            .then().block()
    }

    @PostConstruct
    fun setUp() {
        sender = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            Numeric.toBigInt("0x0a2853fac2c0a03f463f04c4567839473c93f3307da459132b7dd1ca633c0e16"),
            BigInteger.valueOf(8000000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )

        consumer = createConsumer()
    }

    protected fun Mono<Word>.verifyError(): TransactionReceipt {
        val receipt = waitReceipt()
        Assertions.assertFalse(receipt.success())
        return receipt
    }

    protected fun Mono<Word>.verifySuccess(): TransactionReceipt {
        val receipt = waitReceipt()
        Assertions.assertTrue(receipt.success()) {
            val result = ethereum.executeRaw(Request(1, "trace_replayTransaction", Lists.toScala(
                receipt.transactionHash().toString(),
                Lists.toScala("trace", "stateDiff")
            ), "2.0")).block()!!
            "traces: ${result.result().get()}"
        }
        return receipt
    }

    protected suspend fun <T> saveItemHistory(data: T, token: Address = AddressFactory.create(), transactionHash: Word = WordFactory.create(), logIndex: Int? = null, status: LogEventStatus = LogEventStatus.CONFIRMED): T {
        if (data is OrderExchangeHistory) {
            val log = exchangeHistoryRepository.save(LogEvent(data = data, address = token, topic = WordFactory.create(), transactionHash = transactionHash, from = null, nonce = null, status = status, index = 0, logIndex = logIndex, minorLogIndex = 0)).awaitFirst()
            return log.data as T
        }
        throw IllegalArgumentException("Unsupported history type")
    }

    private fun createConsumer(): RaribleKafkaConsumer<ActivityDto> {
        return RaribleKafkaConsumer(
            clientId = "test-consumer-order-activity",
            consumerGroup = "test-group-order-activity",
            valueDeserializerClass = JsonDeserializer::class.java,
            valueClass = ActivityDto::class.java,
            defaultTopic = ActivityTopicProvider.getTopic(application.name, orderIndexerProperties.blockchain.name.toLowerCase()),
            bootstrapServers = kafkaContainer.kafkaBoostrapServers(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST
        )
    }

    protected suspend fun checkActivityWasPublished(orderLeft: Order, topic: Word, activityType: Class<out OrderActivityDto>) = coroutineScope {
        val logEvent = exchangeHistoryRepository.findLogEvents(orderLeft.hash, null).awaitFirst()
        assertThat(logEvent.topic).isEqualTo(topic)

        val events = Collections.synchronizedList(ArrayList<KafkaMessage<ActivityDto>>())

        val job = async {
            consumer
                .receive()
                .collect { events.add(it) }
        }
        Wait.waitAssert {
            assertThat(events)
                .hasSizeGreaterThanOrEqualTo(1)
                .satisfies {
                    val event = it.firstOrNull { event -> event.value.id == logEvent.id.toString() }
                    val activity = event?.value
                    assertThat(activity?.javaClass).isEqualTo(activityType)

                    when (activity) {
                        is OrderActivityMatchDto -> {
                            assertThat(activity.left.hash).isEqualTo(orderLeft.hash)
                        }
                        is  OrderActivityCancelBidDto -> {
                            assertThat(activity.hash).isEqualTo(orderLeft.hash)
                        }
                        is  OrderActivityCancelListDto -> {
                            assertThat(activity.hash).isEqualTo(orderLeft.hash)
                        }
                        else -> Assertions.fail<String>("Unexpected event type ${activity?.javaClass}")
                    }
                }
        }
        job.cancel()
    }
}
