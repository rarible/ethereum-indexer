package com.rarible.protocol.nft.core.service.item.meta.properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.protocol.nft.core.model.ItemId
import com.rarible.protocol.nft.core.service.item.meta.descriptors.BASE_64_JSON_PREFIX
import com.rarible.protocol.nft.core.service.item.meta.descriptors.JSON_PREFIX
import com.rarible.protocol.nft.core.service.item.meta.descriptors.base64MimeToBytes
import com.rarible.protocol.nft.core.service.item.meta.logMetaLoading

object JsonPropertiesParser {

    private val mapper = ObjectMapper().registerKotlinModule()

    fun parse(itemId: ItemId, data: String): ObjectNode? {
        return when {
            data.startsWith(BASE_64_JSON_PREFIX) -> parseBase64(itemId, data.removePrefix(BASE_64_JSON_PREFIX))
            data.startsWith(JSON_PREFIX) -> parseJson(itemId, data.removePrefix(JSON_PREFIX))
            isRawJson(data.trim()) -> parseJson(itemId, data)
            else -> null
        }
    }

    private fun isRawJson(data: String): Boolean {
        return (data.startsWith("{") && data.endsWith("}"))
    }

    private fun parseBase64(itemId: ItemId, data: String): ObjectNode? {
        logMetaLoading(itemId, "parsing properties as Base64")
        val decodedJson = try {
            String(base64MimeToBytes(data))
        } catch (e: Exception) {
            logMetaLoading(itemId, "failed to decode Base64: ${e.message}", warn = true)
            return null
        }
        return parseJson(itemId, decodedJson)
    }

    private fun parseJson(itemId: ItemId, data: String): ObjectNode? {
        return try {
            mapper.readTree(data) as ObjectNode
        } catch (e: Exception) {
            logMetaLoading(itemId, "failed to parse properties from json: ${e.message}", warn = true)
            return null
        }
    }

}