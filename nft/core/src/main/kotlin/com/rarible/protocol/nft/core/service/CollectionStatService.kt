package com.rarible.protocol.nft.core.service

import com.rarible.core.common.nowMillis
import com.rarible.core.common.optimisticLock
import com.rarible.protocol.nft.core.model.CollectionStat
import com.rarible.protocol.nft.core.model.OwnershipFilter
import com.rarible.protocol.nft.core.model.OwnershipFilterByCollection
import com.rarible.protocol.nft.core.repository.CollectionStatRepository
import com.rarible.protocol.nft.core.repository.ownership.OwnershipFilterCriteria.toCriteria
import com.rarible.protocol.nft.core.repository.ownership.OwnershipRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import scalether.domain.Address
import java.time.Duration

@Component
class CollectionStatService(
    private val ownerRepository: OwnershipRepository,
    private val collectionStatRepository: CollectionStatRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun getOrSchedule(token: Address): CollectionStat = optimisticLock {
        val stat = collectionStatRepository.get(token)
        stat ?: collectionStatRepository.save(CollectionStat.empty(token))
    }

    suspend fun updateOld(batchSize: Int, timeOffset: Duration): List<CollectionStat> {
        val oldStats = collectionStatRepository.findOld(batchSize, timeOffset)
        return coroutineScope {
            oldStats.map {
                async {
                    updateStat(it.id)
                }
            }.awaitAll()
        }
    }

    private suspend fun updateStat(token: Address): CollectionStat {
        val filter = OwnershipFilterByCollection(OwnershipFilter.Sort.LAST_UPDATE, token)

        val result = ownerRepository.searchAsFlow(filter.toCriteria(null, null))
            .map { Pair(it.value.value, setOf(it.owner)) }
            .reduce { p1, p2 ->
                Pair(p1.first + p2.first, p1.second + p2.second)
            }

        return optimisticLock {
            val exist = collectionStatRepository.get(token)
            val stat = CollectionStat(
                version = exist?.version,
                id = token,
                lastUpdatedAt = nowMillis(),
                totalItemSupply = result.first,
                totalOwnerCount = result.second.size
            )
            val updated = collectionStatRepository.save(stat)
            logger.info(
                "Updated collection stat for {}: totalItemSupply = {}, totalOwnerCount = {}",
                updated.id, updated.totalItemSupply, updated.totalOwnerCount
            )
            updated
        }
    }

}