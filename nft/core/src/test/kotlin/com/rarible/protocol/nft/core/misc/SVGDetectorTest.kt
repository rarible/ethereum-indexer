package com.rarible.protocol.nft.core.misc

import com.rarible.protocol.nft.core.misc.detector.SVGDetector
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Paths

class SVGDetectorTest {

    private val svgUrl = "https://rarible.mypinata.cloud/data:image/svg+xml;utf8,<svg%20class='nft'><rect%20class='c217'%20x='10'%20y='12'%20width='2'%20height='1'/></svg>"
    private val decodedSvg = "<svg%20class='nft'><rect%20class='c217'%20x='10'%20y='12'%20width='2'%20height='1'/></svg>"

    @Test
    fun `svg detector do not react to strings without svg tag`() {
        val sVGDetector = SVGDetector("url")
        val result = sVGDetector.canDecode()
        Assertions.assertEquals(false, result)
    }

    @Test
    fun `svg detector is able to recognize svg tag`() {
        val sVGDetector = SVGDetector(svgUrl)
        val result = sVGDetector.canDecode()
        Assertions.assertEquals(true, result)
    }

    @Test
    fun `get svg image parts`() {
        val base64 = SVGDetector(svgUrl)

        assertThat(base64.getData()).isEqualTo(decodedSvg)
        assertThat(base64.getMimeType()).isEqualTo("image/svg+xml")
    }

    @Test
    fun `can decode svg images`() {
        val svg = String(Files.readAllBytes(Paths.get(this::class.java.getResource("/svg/test.svg").toURI())));

        val sVGDetector = SVGDetector(svg)
        val result = sVGDetector.canDecode()
        assertThat(result).isTrue
        assertThat(sVGDetector.getData()).isNotEmpty
    }
}
