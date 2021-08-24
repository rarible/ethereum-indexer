package com.rarible.protocol.order.core.configuration

import com.rarible.ethereum.domain.EthUInt256
import com.rarible.ethereum.nft.domain.EIP712DomainNftFactory
import com.rarible.ethereum.nft.validation.LazyNftValidator
import com.rarible.ethereum.sign.domain.EIP712Domain
import com.rarible.ethereum.sign.service.ERC1271SignService
import com.rarible.protocol.order.core.provider.ProtocolCommissionProvider
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.math.BigInteger

@Configuration
@EnableConfigurationProperties(OrderIndexerProperties::class)
class OrderIndexerPropertiesConfiguration(
    private val indexerProperties: OrderIndexerProperties
) {
    @Bean
    fun exchangeContractAddresses() = indexerProperties.exchangeContractAddresses

    @Bean
    fun transferProxyAddresses() = indexerProperties.transferProxyAddresses

    @Bean
    fun eip712Domain(): EIP712Domain {
        return with(indexerProperties) {
            EIP712Domain(
                eip712DomainName,
                eip712DomainVersion,
                BigInteger.valueOf(chainId.toLong()),
                exchangeContractAddresses.v2
            )
        }
    }

    @Bean
    fun protocolCommissionProvider(): ProtocolCommissionProvider {
       return ProtocolCommissionProvider(EthUInt256.of(indexerProperties.protocolCommission.toLong()))
    }

    @Bean
    fun daonomicLazyNftValidator(erc1271SignService: ERC1271SignService): LazyNftValidator {
        return LazyNftValidator(
            erc1271SignService,
            EIP712DomainNftFactory(BigInteger.valueOf(indexerProperties.chainId.toLong()))
        )
    }
}