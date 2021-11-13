package com.rarible.protocol.order.listener.service.descriptors.exchange.v2

import com.rarible.contracts.test.erc1155.TestERC1155
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc721.TestERC721
import com.rarible.ethereum.sign.domain.EIP712Domain
import com.rarible.ethereum.sign.service.ERC1271SignService
import com.rarible.protocol.contracts.common.TransferProxy
import com.rarible.protocol.contracts.erc20.proxy.ERC20TransferProxy
import com.rarible.protocol.contracts.exchange.v2.ExchangeV2
import com.rarible.protocol.contracts.royalties.TestRoyaltiesProvider
import com.rarible.protocol.order.core.service.PrepareTxService
import com.rarible.protocol.order.listener.integration.AbstractIntegrationTest
import com.rarible.protocol.order.listener.misc.setField
import com.rarible.protocol.order.listener.service.order.SideMatchTransactionProvider
import io.daonomic.rpc.domain.Word
import io.mockk.clearMocks
import io.mockk.coEvery
import org.apache.commons.lang3.RandomUtils
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scalether.domain.Address
import scalether.transaction.MonoGasPriceProvider
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import java.math.BigInteger

// TODO: remove this "withOnChainSupport" flag when on-chain orders are merged into master in the protocol-contracts
abstract class AbstractExchangeV2Test(val withOnChainSupport: Boolean = false) : AbstractIntegrationTest() {
    protected lateinit var userSender1: MonoSigningTransactionSender
    protected lateinit var userSender2: MonoSigningTransactionSender
    protected lateinit var token1: TestERC20
    protected lateinit var token2: TestERC20
    protected lateinit var token721: TestERC721
    protected lateinit var token1155: TestERC1155
    protected lateinit var transferProxy: TransferProxy
    protected lateinit var erc20TransferProxy: ERC20TransferProxy
    protected lateinit var exchange: ExchangeV2
    protected lateinit var exchangeWithOnChain: com.rarible.protocol.contracts.exchange.v2.onChain.ExchangeV2
    protected lateinit var eip712Domain: EIP712Domain
    protected lateinit var privateKey1: BigInteger
    protected lateinit var privateKey2: BigInteger

    @Autowired
    private lateinit var exchV2CancelDescriptor: ExchangeV2CancelDescriptor
    @Autowired
    private lateinit var exchangeOrderMatchDescriptor: ExchangeOrderMatchDescriptor
    @Autowired
    private lateinit var exchangeV2UpsertOrderDescriptor: ExchangeV2UpsertOrderDescriptor
    @Autowired
    private lateinit var sideMatchTransactionProvider: SideMatchTransactionProvider
    @Autowired
    protected lateinit var prepareTxService: PrepareTxService
    @Autowired
    protected lateinit var erc721SignService: ERC1271SignService

    @BeforeEach
    fun before() {
        privateKey1 = Numeric.toBigInt(RandomUtils.nextBytes(32))
        privateKey2 = Numeric.toBigInt(RandomUtils.nextBytes(32))

        userSender1 = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            privateKey1,
            BigInteger.valueOf(8000000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )
        userSender2 = MonoSigningTransactionSender(
            ethereum,
            MonoSimpleNonceProvider(ethereum),
            privateKey2,
            BigInteger.valueOf(8000000),
            MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
        )
        token1 = TestERC20.deployAndWait(sender, poller, "Test1", "TST1").block()!!
        token2 = TestERC20.deployAndWait(sender, poller, "Test2", "TST2").block()!!
        token721 = TestERC721.deployAndWait(sender, poller, "Test", "TST").block()!!
        token1155 = TestERC1155.deployAndWait(sender, poller, "ipfs:/").block()!!
        transferProxy = TransferProxy.deployAndWait(sender, poller).block()!!
        erc20TransferProxy = ERC20TransferProxy.deployAndWait(sender, poller).block()!!
        val royaltiesProvider = TestRoyaltiesProvider.deployAndWait(sender, poller).block()!!
        val exchangeAddress: Address
        if (withOnChainSupport) {
            exchangeWithOnChain = com.rarible.protocol.contracts.exchange.v2.onChain.ExchangeV2.deployAndWait(sender, poller).block()!!
            exchangeWithOnChain.__ExchangeV2_init(
                transferProxy.address(),
                erc20TransferProxy.address(),
                BigInteger.ZERO,
                sender.from(),
                royaltiesProvider.address()
            ).execute().verifySuccess()
            exchangeAddress = exchangeWithOnChain.address()
        } else {
            exchange = ExchangeV2.deployAndWait(sender, poller).block()!!
            exchange.__ExchangeV2_init(
                transferProxy.address(),
                erc20TransferProxy.address(),
                BigInteger.ZERO,
                sender.from(),
                royaltiesProvider.address()
            ).execute().verifySuccess()
            exchangeAddress = exchange.address()
        }


        transferProxy.addOperator(exchangeAddress).execute().verifySuccess()
        erc20TransferProxy.addOperator(exchangeAddress).execute().verifySuccess()
        token1.approve(erc20TransferProxy.address(), BigInteger.TEN.pow(10)).withSender(userSender1).execute()
            .verifySuccess()
        token2.approve(erc20TransferProxy.address(), BigInteger.TEN.pow(10)).withSender(userSender2).execute()
            .verifySuccess()
        token721.setApprovalForAll(transferProxy.address(), true).withSender(userSender1).execute().verifySuccess()
        token721.setApprovalForAll(transferProxy.address(), true).withSender(userSender2).execute().verifySuccess()
        token1155.setApprovalForAll(transferProxy.address(), true).withSender(userSender1).execute().verifySuccess()
        token1155.setApprovalForAll(transferProxy.address(), true).withSender(userSender2).execute().verifySuccess()

        setField(exchV2CancelDescriptor, "exchangeContract", exchangeAddress)
        setField(exchangeOrderMatchDescriptor, "exchangeContract", exchangeAddress)
        setField(exchangeV2UpsertOrderDescriptor, "exchangeContract", exchangeAddress)
        setField(sideMatchTransactionProvider, "exchangeContract", exchangeAddress)

        eip712Domain = EIP712Domain(
            name = "Exchange",
            version = "2",
            chainId = BigInteger.valueOf(17),
            verifyingContract = exchangeAddress
        )
        setField(prepareTxService, "eip712Domain", eip712Domain)

        clearMocks(erc721SignService)
        coEvery { erc721SignService.isSigner(any(), any() as Word, any()) } returns true
    }
}
