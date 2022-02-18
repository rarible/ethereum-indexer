package com.rarible.protocol.nft.core.repository

import com.rarible.core.apm.CaptureSpan
import com.rarible.core.apm.SpanType
import com.rarible.core.mongo.query.fast
import com.rarible.protocol.nft.core.model.TokenId
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component

@Component
@CaptureSpan(type = SpanType.DB)
class TokenIdRepository(
    private val mongo: ReactiveMongoOperations
) {
    suspend fun save(tokenId: TokenId): TokenId {
        return mongo.save(tokenId).awaitFirst()
    }

    suspend fun generateTokenId(id: String): Long {
        return mongo.findAndModify(
            Query(Criteria.where("id").`is`(id)).fast(),
            Update().inc("value", 1),
            FindAndModifyOptions.options().returnNew(true).upsert(true),
            TokenId::class.java
        ).awaitFirst().value
    }
}
