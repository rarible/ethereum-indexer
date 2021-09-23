package com.rarible.protocol.nftorder.listener.evaluator

import com.rarible.protocol.nftorder.core.model.ShortOrder

object BestSellOrderComparator : BestOrderComparator {

    override val name: String = "BestSellOrder"

    override fun compare(current: ShortOrder, updated: ShortOrder): ShortOrder {
        val currentTakePrice = current.takePriceUsd
        val updatedTakePrice = updated.takePriceUsd

        val isCurrentTakePriceGreater = when {
            currentTakePrice == null -> true
            updatedTakePrice != null -> currentTakePrice >= updatedTakePrice
            else -> false
        }

        // We have new price, which is lower, then current - updated order is better, using it
        return if (isCurrentTakePriceGreater) updated else current
    }

}