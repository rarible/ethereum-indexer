package com.rarible.protocol.nft.core.service.action

import com.rarible.core.apm.CaptureSpan
import com.rarible.core.apm.SpanType
import com.rarible.core.daemon.sequential.ConsumerEventHandler
import com.rarible.protocol.nft.core.model.ActionEvent
import com.rarible.protocol.nft.core.model.ActionState
import com.rarible.protocol.nft.core.model.BurnItemAction
import com.rarible.protocol.nft.core.model.BurnItemActionEvent
import com.rarible.protocol.nft.core.repository.action.NftItemActionEventRepository
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock

@Component
@CaptureSpan(SpanType.APP)
class InternalActionHandler(
    private val nftItemActionEventRepository: NftItemActionEventRepository,
    private val clock: Clock
) : ConsumerEventHandler<ActionEvent> {

    override suspend fun handle(event: ActionEvent) = when(event) {
        is BurnItemActionEvent -> {
            handleBurnActionEvent(event)
        }
    }

    private suspend fun handleBurnActionEvent(event: BurnItemActionEvent) {
        val existedActions = nftItemActionEventRepository.findByItemIdAndType(event.itemId(), event.type)
        val lastUpdatedAt = clock.instant()
        val burnItemAction = BurnItemAction(
            token = event.token,
            tokenId = event.tokenId,
            createdAt = lastUpdatedAt,
            lastUpdatedAt = lastUpdatedAt,
            state = ActionState.PENDING,
            burnAt = event.burnAt
        )
        val (burnAction, needSave) = if (existedActions.isNotEmpty()) {
            val existedAction = existedActions.single() as BurnItemAction
            if (event.burnAt > existedAction.burnAt) {
                existedAction.copy(
                    burnAt = event.burnAt,
                    state = ActionState.PENDING
                ) to true
            } else {
                existedAction to false
            }
        } else {
            burnItemAction to true
        }
        if (needSave) {
            nftItemActionEventRepository.save(burnAction).awaitFirst()
            logger.info("Save action for item ${event.itemId().decimalStringValue}: $burnAction")
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(InternalActionHandler::class.java)
    }
}