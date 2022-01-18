package com.rarible.protocol.order.core.trace

import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import io.daonomic.rpc.mono.WebClientTransport
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import scalether.core.MonoEthereum
import scalether.domain.Address

class OpenEthereumTransactionTraceTest {

    @Test
    @Tag("manual")
    @Disabled
    fun `find trace work for openethereum`() = runBlocking<Unit> {
        val ethereum = MonoEthereum(WebClientTransport("https://node-mainnet.rarible.com", MonoEthereum.mapper(), 10000, 10000))
        val testing = OpenEthereumTransactionTraceProvider(ethereum)
        val traceResult = testing.traceAndFindFirstCallTo(
            Word.apply("0x237297ec53bf5bb32e949853e7ffeae0663916885236a8bc33b5f5fbc1209197"),
            Address.apply("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b"),
            Binary.apply("0xab834bab")
        )
        assertThat(traceResult?.input)
            .isEqualTo(Binary.apply("0xab834bab0000000000000000000000007be8076f4ea4a4ad08075c2508e481d6c946d12b0000000000000000000000000a267cf51ef038fc00e71801f5a524aec06e4f070000000000000000000000009dcf985075d94b3bf8eddb39d8b611d0d04888b800000000000000000000000000000000000000000000000000000000000000000000000000000000000000005bdf397bb2912859dbd8011f320a222f79a28d2e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007be8076f4ea4a4ad08075c2508e481d6c946d12b0000000000000000000000009dcf985075d94b3bf8eddb39d8b611d0d04888b800000000000000000000000000000000000000000000000000000000000000000000000000000000000000005b3256965e7c3cf26e11fcaf296dfc8807c010730000000000000000000000005bdf397bb2912859dbd8011f320a222f79a28d2e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000037a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006eccddb2eeb800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000061946eac00000000000000000000000000000000000000000000000000000000628294880000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000000037a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006eccddb2eeb800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000061944ca2000000000000000000000000000000000000000000000000000000006282948808dbc3f4d3ade77c5e5bbd6b872337fbf35331bfb5f46ab540754fb33be41e570000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006a0000000000000000000000000000000000000000000000000000000000000074000000000000000000000000000000000000000000000000000000000000007e00000000000000000000000000000000000000000000000000000000000000880000000000000000000000000000000000000000000000000000000000000092000000000000000000000000000000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004fada030dd86b8ff3921be9f2f33b0ad7b0cc8b172cb7f72552daaa6a413840678b8595f44e4179cf638da6695dc32ed52a8ec0875fc5ead0c2780b735e1044fe43aa28716b0b7531293557d5397f8b12f3f5abc000000000000000000000000000000000000000000000000000000000000000000000000000000000000006423b872dd00000000000000000000000000000000000000000000000000000000000000000000000000000000000000008d586f380846dca988cb3b345231af02f989c4110000000000000000000000000000000000000000000000000000000000000f2a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006423b872dd0000000000000000000000009dcf985075d94b3bf8eddb39d8b611d0d04888b800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f2a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006400000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064000000000000000000000000000000000000000000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
    }
}
