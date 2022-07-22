package com.rarible.protocol.nft.core.service.item

import com.rarible.core.apm.CaptureSpan
import com.rarible.core.apm.SpanType
import com.rarible.core.common.orNull
import com.rarible.core.common.toOptional
import com.rarible.core.logging.LoggingUtils
import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.listener.log.domain.LogEvent
import com.rarible.ethereum.listener.log.domain.LogEventStatus
import com.rarible.protocol.nft.core.configuration.NftIndexerProperties
import com.rarible.protocol.nft.core.model.BurnItemAction
import com.rarible.protocol.nft.core.model.BurnItemLazyMint
import com.rarible.protocol.nft.core.model.FeatureFlags
import com.rarible.protocol.nft.core.model.HistoryLog
import com.rarible.protocol.nft.core.model.Item
import com.rarible.protocol.nft.core.model.ItemCreators
import com.rarible.protocol.nft.core.model.ItemHistory
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.model.ItemLazyMint
import com.rarible.protocol.nft.core.model.ItemRoyalty
import com.rarible.protocol.nft.core.model.ItemTransfer
import com.rarible.protocol.nft.core.model.Ownership
import com.rarible.protocol.nft.core.model.Part
import com.rarible.protocol.nft.core.model.ReduceSkipTokens
import com.rarible.protocol.nft.core.repository.action.NftItemActionEventRepository
import com.rarible.protocol.nft.core.repository.history.LazyNftItemHistoryRepository
import com.rarible.protocol.nft.core.repository.history.NftItemHistoryRepository
import com.rarible.protocol.nft.core.repository.item.ItemRepository
import com.rarible.protocol.nft.core.service.RoyaltyService
import com.rarible.protocol.nft.core.service.ownership.OwnershipService
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import scalether.domain.Address

@ExperimentalCoroutinesApi
@Service
@CaptureSpan(type = SpanType.APP)
class ItemReduceServiceV1(
    private val itemRepository: ItemRepository,
    private val ownershipService: OwnershipService,
    private val historyRepository: NftItemHistoryRepository,
    private val lazyHistoryRepository: LazyNftItemHistoryRepository,
    private val nftItemActionEventRepository: NftItemActionEventRepository,
    private val itemCreatorService: ItemCreatorService,
    private val eventListenerListener: ReduceEventListenerListener,
    private val skipTokens: ReduceSkipTokens,
    private val royaltyService: RoyaltyService,
    private val featureFlags: FeatureFlags,
    scannerProperties: NftIndexerProperties.ScannerProperties
) : ItemReduceService {

    private val logger = LoggerFactory.getLogger(ItemReduceService::class.java)
    private val senderCreatedTokens = scannerProperties.senderCreatedTokens.map(Address::apply).toHashSet()

    override fun onItemHistories(logs: List<LogEvent>): Mono<Void> {
        return LoggingUtils.withMarker { marker ->
            if (logs.isNotEmpty()) {
                logger.info(marker, "onItemHistories ${logs.size} logs")
            }
            logs.toFlux()
                .map { it.data as ItemHistory }
                .filter { skipTokens.allowReducing(it.token, it.tokenId) }
                .flatMap { Flux.just(Pair(it.token, it.tokenId)) }
                .distinct()
                .flatMap { update(token = it.first, tokenId = it.second) }
                .then()
        }
    }

    override fun update(token: Address?, tokenId: EthUInt256?, from: ItemId?, to: ItemId?): Flux<ItemId> {
        logger.info("Update token=$token, tokenId=$tokenId")
        val mergedHistories = Flux.mergeOrdered(
            compareBy<HistoryLog>(
                { it.item.token.toString() },
                { it.item.tokenId },
                { it.log.blockNumber },
                { it.log.logIndex }
            ),
            historyRepository.findItemsHistory(token, tokenId, from = from, to = to),
            findLazyItemsHistory(token, tokenId, from)
        )
        return LoggingUtils.withMarkerFlux { marker ->
            mergedHistories
                .windowUntilChanged { ItemId(it.item.token, it.item.tokenId) }
                .concatMap {
                    it.switchOnFirst { first, logs ->
                        val log = first.get()
                        if (log != null) {
                            val initial = Item.empty(log.item.token, log.item.tokenId)
                            updateOneItem(marker, initial, logs)
                                .thenReturn(ItemId(initial.token, initial.tokenId))
                        } else {
                            Mono.empty()
                        }
                    }
                }
        }
    }

    private fun updateOneItem(marker: Marker, initial: Item, byItem: Flux<HistoryLog>): Mono<Void> =
        byItem
            .reduce(initial, mutableMapOf(), this::itemReducer, this::ownershipsReducer)
            .flatMap { royalty(it) }
            .flatMap { reduceActions(it) }
            .flatMap { (item, ownerships) ->
                if (item.token != Address.ZERO()) {
                    val fixed = fixOwnerships(ownerships.values)
                    val supply = fixed.map { it.value }.fold(EthUInt256.of(0), EthUInt256::plus)
                    val lazySupply = fixed.map { it.lazyValue }.fold(EthUInt256.ZERO, EthUInt256::plus)
                    val deleted = supply == EthUInt256.ZERO

                    updateItem(item.copy(supply = supply, lazySupply = lazySupply, deleted = deleted))
                        .flatMap { updatedItem ->
                            handleOwnerships(marker, updatedItem, fixed)
                                .collectList()
                                .flatMap { updated ->
                                    saveItem(marker, updatedItem, updated)
                                }
                                .then()
                        }
                } else {
                    Mono.empty()
                }
            }

    private fun reduceActions(initial: Pair<Item, Map<Address, Ownership>>): Mono<Pair<Item, Map<Address, Ownership>>> =
        mono {
            val item = initial.first

            nftItemActionEventRepository
                .find(item.token, item.tokenId).collectList().awaitFirst()
                .filter { action -> action.isActionable() }
                .fold(initial) { acc, action ->
                    when (action) {
                        is BurnItemAction -> {
                            acc.first to acc.second.mapValues { (_, ownership) ->
                                val value = ownership.value
                                val updatedValue = if (value > EthUInt256.ZERO) value - EthUInt256.ONE else value
                                ownership.copy(value = updatedValue)
                            }
                        }
                    }
                }
        }

    private fun royalty(pair: Pair<Item, Map<Address, Ownership>>): Mono<Pair<Item, Map<Address, Ownership>>> = mono {
        val item = pair.first
        if (item.royalties.isEmpty()) {
            val royalty = royaltyService.getRoyaltyDeprecated(item.token, item.tokenId)
            Pair(item.copy(royalties = royalty), pair.second)
        } else {
            pair
        }
    }

    private fun handleOwnerships(marker: Marker, item: Item, ownerships: List<Ownership>): Flux<Ownership> {
        return ownerships.toFlux().flatMap { updateOwnershipAndSave(marker, item, it) }
    }

    private fun fixOwnerships(ownerships: Collection<Ownership>): List<Ownership> {
        return ownerships
            .map { it.copy(value = maxOf(EthUInt256.of(0), it.value)) }
            .fillValueAndLazyValue()
            .filter { it.owner != Address.ZERO() }
    }

    private fun List<Ownership>.fillValueAndLazyValue(): List<Ownership> {
        val totalMintedValue = this.map { it.value }.fold(EthUInt256.ZERO, EthUInt256::plus)

        return this
            .map {
                if (it.lazyValue > EthUInt256.ZERO) {
                    it.copy(lazyValue = it.lazyValue - totalMintedValue)
                } else {
                    it
                }
            }
            .map { it.copy(value = it.value + it.lazyValue) }
    }

    private fun updateItem(item: Item): Mono<Item> {
        return itemCreatorService.getCreator(item.id).toOptional()
            .map { creator ->
                val newCreators = creator.orNull()?.let { listOf(Part.fullPart(it)) } ?: item.creators
                item.copy(creators = newCreators)
            }
    }

    private fun saveItem(marker: Marker, item: Item, ownerships: List<Ownership>): Mono<Item> {
        // TODO: RPN-497 we limit the number of Item.owners to avoid "too big Kafka message" errors.
        //  Here we use a heuristic: those limited owners will be the ones with the highest balance.
        //  Anyway we should come up with a better solution of passing huge 'Item.owners'.
        val withNewOwners = item.copy(owners = ownerships.sortedByDescending { it.value }.map { it.owner })
        logger.info(marker, "Saving updated item: {}", withNewOwners)
        if (ownerships.size >= MAX_OWNERSHIPS_TO_LOG) {
            logger.info(marker, "Big number of ownerships {} in item {}", ownerships.size, item.id.decimalStringValue)
        }
        return itemRepository.save(withNewOwners)
            .flatMap { savedItem ->
                eventListenerListener.onItemChanged(savedItem).thenReturn(savedItem)
            }
    }

    private fun updateOwnershipAndSave(marker: Marker, item: Item, ownership: Ownership): Mono<Ownership> {
        val builtOwnership = buildOwnership(marker, ownership, item)

        return ownershipService.saveIfChanged(marker, builtOwnership).flatMap { saveResult ->
            (if (saveResult.wasSaved) {
                eventListenerListener.onOwnershipChanged(saveResult.ownership)
            } else {
                Mono.empty()
            }).thenReturn(saveResult.ownership)
        }
    }

    private fun buildOwnership(marker: Marker, ownership: Ownership, item: Item): Ownership {
        logger.info(marker, "Build formed ownership: $ownership\nby item: $item")
        return ownership.copy(creators = item.creators)
    }

    private fun itemReducer(item: Item, log: HistoryLog): Item {
        val (event, logEvent) = log
        return when (logEvent.status) {
            LogEventStatus.CONFIRMED -> {
                when (event) {
                    is ItemTransfer -> item.safeProcessTransfer(event, logEvent.from)
                    is ItemRoyalty -> {
                        logger.info("Ignoring ItemRoyalty event: $event")
                        item
                    }
                    is ItemLazyMint -> {
                        item.copy(
                            royalties = event.royalties,
                            creators = event.creators,
                            creatorsFinal = true,
                            isRaribleContract = true
                        )
                    }
                    is ItemCreators -> {
                        item.copy(creators = event.creators, creatorsFinal = true, isRaribleContract = true)
                    }
                    is BurnItemLazyMint -> {
                        logger.info("Ignoring BurnItemLazyMint event: $event")
                        item
                    }
                }.copy(date = event.date)
            }
            else -> item
        }
    }

    /**
     * Makes sure the creator is not forged by artificial contract events.
     */
    private fun Item.safeProcessTransfer(itemTransfer: ItemTransfer, transactionSender: Address?): Item {
        val creators = if (
            itemTransfer.from == Address.ZERO()
            && creators.isEmpty()
            && !creatorsFinal
        ) {
            val isSenderCreatedTokens = token in senderCreatedTokens
            if ((featureFlags.validateCreatorByTransactionSender || isSenderCreatedTokens)
                && transactionSender != null
                && itemTransfer.owner != transactionSender
            ) {
                if (isSenderCreatedTokens) listOf(Part.fullPart(transactionSender)) else this.creators
            } else {
                listOf(Part.fullPart(itemTransfer.owner))
            }
        } else {
            this.creators
        }
        return if (this.mintedAt == null && itemTransfer.from == Address.ZERO()) {
            this.copy(creators = creators, mintedAt = itemTransfer.date)
        } else {
            this.copy(creators = creators)
        }
    }

    private fun ownershipsReducer(
        map: MutableMap<Address, Ownership>,
        log: HistoryLog
    ): MutableMap<Address, Ownership> {
        val (event, _) = log
        return when (event) {
            is ItemTransfer -> {
                val from = map.getOrElse(event.from) { Ownership.empty(event.token, event.tokenId, event.from) }
                val to = map.getOrElse(event.owner) { Ownership.empty(event.token, event.tokenId, event.owner) }
                map.apply {
                    put(event.from, ownershipReducer(from, log))
                    put(event.owner, ownershipReducer(to, log))
                }
            }
            is ItemLazyMint -> {
                val to = map.getOrElse(event.owner) { Ownership.empty(event.token, event.tokenId, event.owner) }
                map.apply {
                    put(event.owner, ownershipReducer(to, log))
                }
            }
            is ItemRoyalty -> {
                map
            }
            is ItemCreators -> {
                map
            }
            is BurnItemLazyMint -> {
                val from = map.getOrElse(event.from) { Ownership.empty(event.token, event.tokenId, event.from) }
                val to = Ownership.empty(event.token, event.tokenId, event.owner)
                map.apply {
                    put(event.from, ownershipReducer(from, log))
                    put(event.owner, ownershipReducer(to, log))
                }
            }
        }
    }

    private fun ownershipReducer(ownership: Ownership, log: HistoryLog): Ownership {
        val (event, status) = log
        return when (status.status) {
            LogEventStatus.CONFIRMED ->
                when (event) {
                    is ItemTransfer -> {
                        when (ownership.owner) {
                            event.owner -> if (ownership.owner != event.from) {
                                ownership.copy(value = ownership.value + event.value)
                            } else {
                                ownership
                            }
                            event.from -> if (ownership.owner != Address.ZERO()) {
                                ownership.copy(value = ownership.value - event.value)
                            } else {
                                ownership
                            }
                            else -> ownership
                        }
                    }
                    is ItemLazyMint -> {
                        ownership.copy(lazyValue = event.value)
                    }
                    is ItemRoyalty -> {
                        ownership
                    }
                    is ItemCreators -> {
                        ownership
                    }
                    is BurnItemLazyMint -> {
                        if (ownership.lazyValue > EthUInt256.ZERO) {
                            ownership.copy(lazyValue = ownership.lazyValue - event.value)
                        } else {
                            ownership
                        }
                    }
                }.copy(date = event.date)
            else -> ownership
        }
    }

    private fun findLazyItemsHistory(token: Address?, tokenId: EthUInt256?, from: ItemId?): Flux<HistoryLog> {
        return lazyHistoryRepository.find(token, tokenId, from).map {
            HistoryLog(
                item = it,
                log = LogEvent(
                    data = it,
                    address = Address.ZERO(),
                    topic = WORD_ZERO,
                    transactionHash = WORD_ZERO,
                    status = LogEventStatus.CONFIRMED,
                    blockNumber = -1,
                    logIndex = -1,
                    index = 0,
                    minorLogIndex = 0
                )
            )
        }
    }

    companion object {
        const val MAX_OWNERSHIPS_TO_LOG = 1000
        val WORD_ZERO: Word = Word.apply("0x0000000000000000000000000000000000000000000000000000000000000000")

        val logger: Logger = LoggerFactory.getLogger(ItemReduceService::class.java)

        private fun <T, A1, A2> Flux<T>.reduce(
            a1: A1,
            a2: A2,
            reducer1: (A1, T) -> A1,
            reducer2: (A2, T) -> A2
        ): Mono<Pair<A1, A2>> =
            reduce(Pair(a1, a2)) { (ac1, ac2), t ->
                Pair(reducer1(ac1, t), reducer2(ac2, t))
            }
    }
}
