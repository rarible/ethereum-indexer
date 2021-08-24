package com.rarible.protocol.order.core.service.validation

import com.rarible.ethereum.sign.domain.EIP712Domain
import com.rarible.ethereum.sign.service.ERC1271SignService
import com.rarible.protocol.order.core.model.Order
import com.rarible.protocol.order.core.model.Order.Companion.legacyMessage
import com.rarible.protocol.order.core.model.OrderType
import com.rarible.protocol.order.core.model.OrderVersion
import com.rarible.protocol.order.core.service.CommonSigner
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class OrderSignatureValidator(
    private val eip712Domain: EIP712Domain,
    private val legacySigner: CommonSigner,
    private val erc1271SignService: ERC1271SignService
) {
    @Throws(IncorrectSignatureException::class)
    suspend fun validate(orderVersion: OrderVersion) {
        val signature = orderVersion.signature ?: throw IncorrectSignatureException()

        when (orderVersion.type) {
            OrderType.RARIBLE_V1 -> {
                logger.info("validating legacy order message: ${orderVersion.hash}, signature: $signature")
                val legacyMessage = orderVersion.legacyMessage()
                val signer = legacySigner.recover(legacyMessage, signature)
                if (orderVersion.maker != signer) {
                    throw IncorrectSignatureException()
                }
            }
            OrderType.RARIBLE_V2 -> {
                logger.info("validating v2 order message: ${orderVersion.hash}, signature: $signature, eip712Domain: $eip712Domain")
                val structHash = Order.hash(orderVersion)
                val hash = eip712Domain.hashToSign(structHash)
                if (erc1271SignService.isSigner(orderVersion.maker, hash, signature).not()) {
                    throw IncorrectSignatureException()
                }
            }
            OrderType.OPEN_SEA_V1 -> Unit
        }
    }

    class IncorrectSignatureException : RuntimeException("Incorrect signature")

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderSignatureValidator::class.java)
    }
}
