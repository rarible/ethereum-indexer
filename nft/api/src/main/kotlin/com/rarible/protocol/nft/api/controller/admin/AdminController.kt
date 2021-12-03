package com.rarible.protocol.nft.api.controller.admin

import com.rarible.core.task.Task
import com.rarible.protocol.nft.api.dto.AdminTaskDto
import com.rarible.protocol.nft.api.dto.TokenDto
import com.rarible.protocol.nft.api.exceptions.EntityNotFoundApiException
import com.rarible.protocol.nft.api.service.admin.ReindexTokenService
import com.rarible.protocol.nft.core.model.Token
import com.rarible.protocol.nft.core.model.TokenStandard
import com.rarible.protocol.nft.core.service.token.TokenUpdateService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import scalether.domain.Address

@RestController
class AdminController(
    private val reindexTokenService: ReindexTokenService,
    private val tokenUpdateService: TokenUpdateService
) {
    @GetMapping(
        value = ["/admin/nft/collections/{collectionId}"],
        produces = ["application/json"]
    )
    suspend fun getTokenById(
        @PathVariable("collectionId") collectionId: Address
    ): ResponseEntity<TokenDto> {
        val token = tokenUpdateService.getToken(collectionId)
            ?: throw EntityNotFoundApiException("Collection", collectionId)
        return ResponseEntity.ok().body(convert(token))
    }

    @DeleteMapping(
        value = ["/admin/nft/collections/{collectionId}"],
        produces = ["application/json"]
    )
    suspend fun deleteTokenById(
        @PathVariable("collectionId") collectionId: Address
    ): ResponseEntity<Void> {
        tokenUpdateService.removeToken(collectionId)
        return ResponseEntity.noContent().build()
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks/reindexToken"],
        produces = ["application/json"]
    )
    suspend fun createReindexTokenTask(
        @RequestParam(value = "collection", required = true) collection: List<Address>,
        @RequestParam(value = "fromBlock", required = false) fromBlock: Long?,
        @RequestParam(value = "force", required = false) force: Boolean?
    ): ResponseEntity<AdminTaskDto> {
        val task = reindexTokenService.createReindexTokenTask(collection, fromBlock, force ?: false)
        return ResponseEntity.ok().body(convert(task))
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks/reindexItems"],
        produces = ["application/json"]
    )
    suspend fun createReindexTokenItemsTask(
        @RequestParam(value = "collection", required = true) collection: List<Address>,
        @RequestParam(value = "fromBlock", required = false) fromBlock: Long?,
        @RequestParam(value = "force", required = false) force: Boolean?
    ): ResponseEntity<AdminTaskDto> {
        val task = reindexTokenService.createReindexTokenItemsTask(collection, fromBlock, force ?: false)
        return ResponseEntity.ok().body(convert(task))
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks/reduceToken"],
        produces = ["application/json"]
    )
    suspend fun createReduceTokenTask(
        @RequestParam(value = "collection", required = true) collection: Address,
        @RequestParam(value = "force", required = false) force: Boolean?
    ): ResponseEntity<AdminTaskDto> {
        val task = reindexTokenService.createReduceTokenTask(collection, force ?: false)
        return ResponseEntity.ok().body(convert(task))
    }

    @PostMapping(
        value = ["/admin/nft/collections/tasks/setTokenStandard"],
        produces = ["application/json"]
    )
    suspend fun setTokenStandard(
        @RequestParam(value = "collection", required = true) collection: Address,
        @RequestParam(value = "standard", required = true) standardStr: String
    ): ResponseEntity<Void> {
        val standard = requireNotNull(TokenStandard.valueOf(standardStr)) {
            "Unknown token standard $standardStr"
        }
        tokenUpdateService.setTokenStandard(collection, standard)
        return ResponseEntity.noContent().build()
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks/reduceItems"],
        produces = ["application/json"]
    )
    suspend fun createReduceTokenItemsTask(
        @RequestParam(value = "collection", required = true) collection: Address,
        @RequestParam(value = "force", required = false) force: Boolean?
    ): ResponseEntity<AdminTaskDto> {
        val task = reindexTokenService.createReduceTokenItemsTask(collection, force ?: false)
        return ResponseEntity.ok().body(convert(task))
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks/reindexItemsRoyalties"],
        produces = ["application/json"]
    )
    suspend fun createReindexTokenItemRoyaltiesTask(
        @RequestParam(value = "collection", required = true) collection: Address,
        @RequestParam(value = "force", required = false) force: Boolean?
    ): ResponseEntity<AdminTaskDto> {
        val task = reindexTokenService.createReindexTokenItemRoyaltiesTask(collection, force ?: false)
        return ResponseEntity.ok().body(convert(task))
    }

    @GetMapping(
        value = ["/admin/nft/collections/tasks"],
        produces = ["application/json"]
    )
    suspend fun getTokenTasks(): ResponseEntity<List<AdminTaskDto>> {
        val tasks = reindexTokenService.getTokenTasks()
        return ResponseEntity.ok().body(tasks.map { convert(it) })
    }

    private fun convert(task: Task): AdminTaskDto {
        return AdminTaskDto(
            id = task.id.toHexString(),
            type = task.type,
            status = task.lastStatus.toString(),
            error = task.lastError,
            params = task.param,
            state = task.state.toString()
        )
    }

    private fun convert(token: Token): TokenDto {
        return TokenDto(
            id = token.id,
            standard = token.standard.name,
            owner = token.owner,
            name = token.name,
            symbol = token.symbol,
            features = token.features.map { it.name }
        )
    }
}
