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

package com.ritense.valtimoplugins.cloudevent.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.ritense.case.service.CaseDefinitionService
import com.ritense.case_.domain.definition.CaseDefinition
import com.ritense.document.domain.Document
import com.ritense.document.domain.impl.JsonSchemaDocumentId
import com.ritense.document.service.DocumentService
import com.ritense.document.service.result.CreateDocumentResult
import com.ritense.inbox.ValtimoEvent
import com.ritense.plugin.domain.PluginProcessLink
import com.ritense.processdocument.domain.ProcessDefinitionCaseDefinition
import com.ritense.processdocument.domain.ProcessDefinitionCaseDefinitionId
import com.ritense.processdocument.domain.ProcessDefinitionId
import com.ritense.processdocument.domain.ProcessDocumentInstance
import com.ritense.processdocument.service.ProcessDefinitionCaseDefinitionService
import com.ritense.processdocument.service.ProcessDocumentAssociationService
import com.ritense.processlink.domain.ActivityTypeWithEventName
import com.ritense.processlink.repository.ValtimoPluginProcessLinkRepository
import com.ritense.valtimo.contract.case_.CaseDefinitionId
import com.ritense.valtimo.service.ProcessPropertyService
import com.ritense.valtimoplugins.cloudevent.BaseTest
import com.ritense.valtimoplugins.cloudevent.repository.ProcessedCloudEventRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.operaton.bpm.engine.RepositoryService
import org.operaton.bpm.engine.RuntimeService
import org.operaton.bpm.engine.repository.ProcessDefinition
import org.operaton.bpm.engine.repository.ProcessDefinitionQuery
import org.operaton.bpm.engine.runtime.MessageCorrelationBuilder
import org.operaton.bpm.engine.runtime.ProcessInstance
import org.operaton.bpm.model.bpmn.BpmnModelInstance
import org.operaton.bpm.model.bpmn.instance.CatchEvent
import org.operaton.bpm.model.bpmn.instance.Message
import org.operaton.bpm.model.bpmn.instance.MessageEventDefinition
import org.semver4j.Semver
import java.util.Optional
import java.util.UUID

/**
 * Covers starting a process from a `receive-cloud-event` link on a message start event: which
 * links start something, and how the started case is tied to its document.
 */
class CloudEventProcessLinkListenerTest : BaseTest() {
    private lateinit var pluginProcessLinkRepository: ValtimoPluginProcessLinkRepository
    private lateinit var runtimeService: RuntimeService
    private lateinit var repositoryService: RepositoryService
    private lateinit var processPropertyService: ProcessPropertyService
    private lateinit var processDefinitionCaseDefinitionService: ProcessDefinitionCaseDefinitionService
    private lateinit var documentService: DocumentService
    private lateinit var processDocumentAssociationService: ProcessDocumentAssociationService
    private lateinit var caseDefinitionService: CaseDefinitionService
    private lateinit var processedCloudEventRepository: ProcessedCloudEventRepository
    private lateinit var listener: CloudEventProcessLinkListener

    @BeforeEach
    fun setUp() {
        pluginProcessLinkRepository = mock()
        runtimeService = mock()
        repositoryService = mock()
        processPropertyService = mock()
        processDefinitionCaseDefinitionService = mock()
        documentService = mock()
        processDocumentAssociationService = mock()
        caseDefinitionService = mock()
        processedCloudEventRepository = mock()

        listener =
            CloudEventProcessLinkListener(
                pluginProcessLinkRepository,
                runtimeService,
                repositoryService,
                processPropertyService,
                processDefinitionCaseDefinitionService,
                documentService,
                processDocumentAssociationService,
                caseDefinitionService,
                ObjectMapper(),
                processedCloudEventRepository,
            )
    }

    /**
     * Starting the process by its definition key would enter it at whichever start event
     * Operaton considers the initial one, which on a process with a plain start event as well is
     * not this one.
     */
    @Test
    fun `should start a case by correlating the start message of the linked element`() {
        val link = linked(caseProcessStartEvent())
        givenACaseCanBeStartedFor(link, DOCUMENT_ID)
        val correlation = givenStartMessageCorrelates()

        listener.handle(event())

        verify(correlation).processDefinitionId(CASE_PROCESS_DEFINITION_ID)
        // Valtimo resolves `doc:` from the business key, and associates the instance with that
        // document at its start event.
        verify(correlation).processInstanceBusinessKey(DOCUMENT_ID.toString())
        verify(correlation).correlateStartMessage()
        verify(processDocumentAssociationService).createProcessDocumentInstance(
            eq(PROCESS_INSTANCE_ID),
            eq(DOCUMENT_ID.id),
            eq("Verwerk cloud event"),
        )
    }

    @Test
    fun `should leave an association Valtimo already made at the start event alone`() {
        // Valtimo's start event listener associates the instance itself, possibly with another
        // document (a building block's). Creating one regardless throws on that.
        val link = linked(caseProcessStartEvent())
        givenACaseCanBeStartedFor(link, DOCUMENT_ID)
        val correlation = givenStartMessageCorrelates()
        doReturn(Optional.of(mock<ProcessDocumentInstance>()))
            .whenever(processDocumentAssociationService)
            .findProcessDocumentInstance(any())

        listener.handle(event())

        verify(correlation).correlateStartMessage()
        verify(processDocumentAssociationService, never()).createProcessDocumentInstance(any(), any(), any())
    }

    @Test
    fun `should skip a case process that may not initialise a document instead of failing`() {
        // Failing would fail the event, and every other link it matched, on every delivery.
        val link = linked(caseProcessStartEvent())
        givenACaseCanBeStartedFor(link, DOCUMENT_ID, canInitializeDocument = false)

        listener.handle(event())

        verify(documentService, never()).createDocument(any())
        verify(runtimeService, never()).createMessageCorrelation(any())
    }

    @Test
    fun `should skip a process that is linked to no case definition`() {
        // An earlier version of a case process, detached when a newer one was deployed.
        val link = linked(caseProcessStartEvent())
        whenever(processPropertyService.isSystemProcessById(link.processDefinitionId)).thenReturn(false)

        listener.handle(event())

        verify(documentService, never()).createDocument(any())
        verify(runtimeService, never()).createMessageCorrelation(any())
    }

    @Test
    fun `should start the latest version of a system process`() {
        linked(systemProcessStartEvent("cloud-event-intake:2:def"))
        givenLatestVersionOf("cloud-event-intake", "cloud-event-intake:2:def")
        val correlation = givenStartMessageCorrelates()

        listener.handle(event())

        verify(correlation).processDefinitionId("cloud-event-intake:2:def")
        verify(correlation).correlateStartMessage()
    }

    @Test
    fun `should not start an earlier version of a system process`() {
        // Valtimo copies the link onto every new version and the old one keeps its own, so
        // without this one event would start an instance of every version ever deployed.
        linked(systemProcessStartEvent("cloud-event-intake:1:abc"))
        givenLatestVersionOf("cloud-event-intake", "cloud-event-intake:2:def")

        listener.handle(event())

        verify(runtimeService, never()).createMessageCorrelation(any())
    }

    private fun linked(link: PluginProcessLink): PluginProcessLink {
        whenever(pluginProcessLinkRepository.findByPluginActionDefinitionKey("receive-cloud-event"))
            .thenReturn(listOf(link))
        return link
    }

    private fun caseProcessStartEvent(): PluginProcessLink =
        mock<PluginProcessLink>().also {
            whenever(it.activityType).thenReturn(ActivityTypeWithEventName.MESSAGE_START_EVENT_START)
            whenever(it.activityId).thenReturn("Event_0beitek")
            whenever(it.processDefinitionId).thenReturn(CASE_PROCESS_DEFINITION_ID)
        }

    private fun systemProcessStartEvent(processDefinitionId: String): PluginProcessLink =
        mock<PluginProcessLink>().also {
            whenever(it.activityType).thenReturn(ActivityTypeWithEventName.MESSAGE_START_EVENT_START)
            whenever(it.activityId).thenReturn("Event_0beitek")
            whenever(it.processDefinitionId).thenReturn(processDefinitionId)
            whenever(processPropertyService.isSystemProcessById(processDefinitionId)).thenReturn(true)
        }

    private fun givenLatestVersionOf(
        key: String,
        latestProcessDefinitionId: String,
    ) {
        val linked = mock<ProcessDefinition>()
        whenever(linked.key).thenReturn(key)
        val latest = mock<ProcessDefinition>()
        whenever(latest.id).thenReturn(latestProcessDefinitionId)
        val query = mock<ProcessDefinitionQuery>()
        whenever(query.processDefinitionKey(key)).thenReturn(query)
        whenever(query.latestVersion()).thenReturn(query)
        whenever(query.list()).thenReturn(listOf(latest))
        whenever(repositoryService.getProcessDefinition(any())).thenReturn(linked)
        whenever(repositoryService.createProcessDefinitionQuery()).thenReturn(query)
    }

    /**
     * Everything between "this link points at a message start event" and "a case may be created
     * for it": the process is not a system process, it belongs to the active version of a case
     * definition, and the document itself is created.
     */
    private fun givenACaseCanBeStartedFor(
        link: PluginProcessLink,
        documentId: JsonSchemaDocumentId,
        canInitializeDocument: Boolean = true,
    ) {
        // Real objects rather than mocks: these are plain JPA entities whose `val`s Mockito
        // cannot stub.
        val caseDefinitionId = CaseDefinitionId("cloudevents", Semver("1.0.0"))
        val processDefinitionCaseDefinition =
            ProcessDefinitionCaseDefinition(
                ProcessDefinitionCaseDefinitionId(
                    ProcessDefinitionId(link.processDefinitionId),
                    caseDefinitionId,
                ),
                canInitializeDocument = canInitializeDocument,
            )
        val caseDefinition =
            CaseDefinition(
                id = caseDefinitionId,
                name = "Cloud events",
                createdDate = null,
                canHaveAssignee = true,
            )

        // Each mock is finished before the next stubbing starts; stubbing one inside another
        // `thenReturn(...)` is what Mockito reports as unfinished stubbing.
        val document = mock<Document>()
        whenever(document.id()).thenReturn(documentId)
        val createResult = mock<CreateDocumentResult>()
        doReturn(Optional.of(document)).whenever(createResult).resultingDocument()
        val processDefinition = mock<ProcessDefinition>()
        whenever(processDefinition.name).thenReturn("Verwerk cloud event")

        whenever(processPropertyService.isSystemProcessById(link.processDefinitionId)).thenReturn(false)
        whenever(processDefinitionCaseDefinitionService.findByProcessDefinitionIdOrNull(any()))
            .thenReturn(processDefinitionCaseDefinition)
        whenever(caseDefinitionService.getActiveCaseDefinition("cloudevents")).thenReturn(caseDefinition)
        whenever(documentService.createDocument(any())).thenReturn(createResult)
        whenever(repositoryService.getProcessDefinition(link.processDefinitionId)).thenReturn(processDefinition)
    }

    /** Stubs the BPMN lookup of the message name and the correlation builder chain. */
    private fun givenStartMessageCorrelates(): MessageCorrelationBuilder {
        val message = mock<Message>()
        whenever(message.name).thenReturn(MESSAGE_NAME)
        val messageEventDefinition = mock<MessageEventDefinition>()
        whenever(messageEventDefinition.message).thenReturn(message)
        val catchEvent = mock<CatchEvent>()
        whenever(catchEvent.eventDefinitions).thenReturn(listOf(messageEventDefinition))
        val model = mock<BpmnModelInstance>()
        doReturn(catchEvent).whenever(model).getModelElementById<CatchEvent>(any())
        whenever(repositoryService.getBpmnModelInstance(any())).thenReturn(model)

        val processInstance = mock<ProcessInstance>()
        whenever(processInstance.id).thenReturn(PROCESS_INSTANCE_ID)
        val correlation = mock<MessageCorrelationBuilder>()
        whenever(runtimeService.createMessageCorrelation(MESSAGE_NAME)).thenReturn(correlation)
        whenever(correlation.processDefinitionId(any())).thenReturn(correlation)
        whenever(correlation.processInstanceBusinessKey(any())).thenReturn(correlation)
        whenever(correlation.setVariables(any())).thenReturn(correlation)
        whenever(correlation.correlateStartMessage()).thenReturn(processInstance)
        return correlation
    }

    private fun event() =
        ValtimoEvent(
            id = "event-1",
            type = "nl.example.zaak.created",
            date = null,
            userId = null,
            roles = null,
            resultType = null,
            resultId = null,
            result = null,
        )

    private companion object {
        private const val CASE_PROCESS_DEFINITION_ID = "Verwerk_cloud_event:1:abc"
        private const val MESSAGE_NAME = "IncomingCloudEvent"
        private const val PROCESS_INSTANCE_ID = "99999999-8888-7777-6666-555555555555"
        private val DOCUMENT_UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")
        private val DOCUMENT_ID = JsonSchemaDocumentId.existingId(DOCUMENT_UUID)
    }
}
