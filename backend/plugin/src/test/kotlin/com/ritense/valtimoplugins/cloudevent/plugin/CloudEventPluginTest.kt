/*
 * Copyright 2026 Ritense BV, the Netherlands.
 *
 * Licensed under EUPL, Version 1.2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ritense.valtimoplugins.cloudevent.plugin

import com.fasterxml.jackson.databind.ObjectMapper
import com.ritense.outbox.OutboxService
import com.ritense.outbox.domain.BaseEvent
import com.ritense.valtimoplugins.cloudevent.BaseTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import java.util.function.Supplier

class CloudEventPluginTest : BaseTest() {
    private val objectMapper = ObjectMapper()
    private lateinit var outboxService: OutboxService
    private lateinit var plugin: CloudEventPlugin

    @BeforeEach
    fun setUp() {
        outboxService = mock()
        plugin = CloudEventPlugin(outboxService, objectMapper)
    }

    @Test
    fun `should publish the type and result fields of the action`() {
        val event = publish(data = null, resultType = "zaak", resultId = "Z-1")

        assertEquals("nl.example.zaak.created", event.type)
        assertEquals("zaak", event.resultType)
        assertEquals("Z-1", event.resultId)
    }

    @Test
    fun `should publish resolved object data as it is`() {
        val event = publish(data = mapOf("zaakId" to "Z-1", "count" to 2))

        assertEquals(objectMapper.readTree("""{"zaakId":"Z-1","count":2}"""), event.result)
    }

    @Test
    fun `should publish JSON entered as text as JSON`() {
        val event = publish(data = """{"zaakId": "Z-1"}""")

        assertEquals(objectMapper.readTree("""{"zaakId":"Z-1"}"""), event.result)
    }

    @Test
    fun `should wrap data that is not a JSON object or array`() {
        assertEquals(objectMapper.readTree("""{"data":"Hoi"}"""), publish(data = "Hoi").result)
        assertEquals(objectMapper.readTree("""{"data":"42"}"""), publish(data = "42").result)
        assertEquals(objectMapper.readTree("""{"data":null}"""), publish(data = null).result)
        assertEquals(objectMapper.readTree("""{"data":null}"""), publish(data = " ").result)
    }

    private fun publish(
        data: Any?,
        resultType: String? = null,
        resultId: String? = null,
    ): BaseEvent {
        plugin.publishCloudEvent("nl.example.zaak.created", resultType, resultId, data)
        val supplier = argumentCaptor<Supplier<BaseEvent>>()
        verify(outboxService, atLeastOnce()).send(supplier.capture())
        return supplier.lastValue.get()
    }
}
