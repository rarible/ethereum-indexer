package com.rarible.protocol.erc20.listener.configuration

import com.rarible.ethereum.domain.Blockchain
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

internal const val RARIBLE_PROTOCOL_LISTENER_STORAGE = "listener"

@ConstructorBinding
@ConfigurationProperties(RARIBLE_PROTOCOL_LISTENER_STORAGE)
class Erc20ListenerProperties(
    val blockchain: Blockchain,
    val tokens: List<String> = emptyList(),
    val ignoredOwners: List<String> = emptyList(),
    val blockCountBeforeSnapshot: Int = 12
)