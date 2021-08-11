package com.rarible.protocol.erc20.listener.service.descriptors

import com.rarible.ethereum.listener.log.LogEventDescriptor
import com.rarible.protocol.erc20.core.model.Erc20TokenHistory
import com.rarible.protocol.erc20.core.repository.Erc20TransferHistoryRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import scalether.domain.response.Log
import java.util.*

@ExperimentalCoroutinesApi
interface Erc20LogEventDescriptor<T : Erc20TokenHistory> : LogEventDescriptor<T> {
    override val collection: String
        get() = Erc20TransferHistoryRepository.COLLECTION

    override fun convert(log: Log, timestamp: Long): Publisher<T> {
        return mono { convert(log, Date(timestamp * 1000)) }.flatMapMany { Flux.fromIterable(it) }
    }

    suspend fun convert(log: Log, date: Date): List<T>
}
