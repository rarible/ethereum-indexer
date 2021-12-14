package com.rarible.protocol.nft.core.converters.model

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.ethereum.listener.log.domain.LogEventStatus
import com.rarible.protocol.nft.core.model.BlockchainEntityEvent

object BlockchainStatusConverter {
    fun convert(source: Log.Status): BlockchainEntityEvent.Status {
        return when (source) {
            Log.Status.CONFIRMED -> BlockchainEntityEvent.Status.CONFIRMED
            Log.Status.PENDING -> BlockchainEntityEvent.Status.PENDING
            Log.Status.REVERTED -> BlockchainEntityEvent.Status.REVERTED
            Log.Status.DROPPED -> BlockchainEntityEvent.Status.DROPPED
            Log.Status.INACTIVE ->  BlockchainEntityEvent.Status.INACTIVE
        }
    }

    fun convert(source: LogEventStatus): Log.Status {
        return when (source) {
            LogEventStatus.CONFIRMED -> Log.Status.CONFIRMED
            LogEventStatus.PENDING -> Log.Status.PENDING
            LogEventStatus.REVERTED -> Log.Status.REVERTED
            LogEventStatus.DROPPED -> Log.Status.DROPPED
            LogEventStatus.INACTIVE -> Log.Status.INACTIVE
        }
    }
}
