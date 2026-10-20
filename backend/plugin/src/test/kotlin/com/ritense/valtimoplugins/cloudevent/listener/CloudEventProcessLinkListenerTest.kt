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
import com.fasterxml.jackson.databind.node.ObjectNode
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
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.operaton.bpm.engine.RepositoryService
import org.operaton.bpm.engine.RuntimeService
import org.operaton.bpm.engine.repository.ProcessDefinition
import org.operaton.bpm.engine.repository.ProcessDefinitionQuery
import org.operaton.bpm.engine.runtime.ActivityInstance
import org.operaton.bpm.engine.runtime.EventSubscription
import org.operaton.bpm.engine.runtime.EventSubscriptionQuery
import org.operaton.bpm.engine.runtime.Execution
import org.operaton.bpm.engine.runtime.ExecutionQuery
import org.operaton.bpm.engine.runtime.MessageCorrelationBuilder
import org.operaton.bpm.engine.runtime.ProcessInstance
import org.operaton.bpm.engine.runtime.ProcessInstanceQuery
import org.operaton.bpm.model.bpmn.BpmnModelInstance
import org.operaton.bpm.model.bpmn.instance.CatchEvent
import org.operaton.bpm.model.bpmn.instance.Message
import org.operaton.bpm.model.bpmn.instance.MessageEventDefinition
import org.operaton.bpm.model.bpmn.instance.Process
import org.semver4j.Semver
import java.time.LocalDateTime
import java.util.Optional
import java.util.UUID

/**
 * Covers routing an incoming cloud event to its `receive-cloud-event` links: deduplication, the
 * event type filter, continuing a waiting execution, and starting a process from a message start
 * event, including how the started case is tied to its document.
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

    @Test
    fun `should record an event and skip it when it is delivered again`() {
        linked(receiveTask())
        val execution = givenWaitingExecution()

        listener.handle(event())
        whenever(processedCloudEventRepository.existsById("event-1")).thenReturn(true)
        listener.handle(event())

        verify(processedCloudEventRepository).saveAndFlush(argThat { eventId == "event-1" })
        verify(runtimeService, times(1)).signal(eq(execution.id), any<Map<String, Any>>())
    }

    @Test
    fun `should not record an event that matches no link`() {
        // The inbox carries Valtimo's own events too, which would otherwise each leave a row.
        linked(receiveTask(eventTypeFilter = "nl.example.zaak.closed"))

        listener.handle(event())

        verify(processedCloudEventRepository, never()).saveAndFlush(any())
    }

    @Test
    fun `should skip a link whose event type filter does not match`() {
        linked(receiveTask(eventTypeFilter = "nl.example.zaak.closed"))
        givenWaitingExecution()

        listener.handle(event())

        verify(runtimeService, never()).signal(any(), any<Map<String, Any>>())
    }

    @Test
    fun `should ignore whitespace around an event type filter`() {
        linked(receiveTask(eventTypeFilter = " nl.example.zaak.created "))
        val execution = givenWaitingExecution()

        listener.handle(event())

        verify(runtimeService).signal(eq(execution.id), any<Map<String, Any>>())
    }

    @Test
    fun `should accept every event on a link with a blank event type filter`() {
        linked(receiveTask(eventTypeFilter = ""))
        val execution = givenWaitingExecution()

        listener.handle(event())

        verify(runtimeService).signal(eq(execution.id), any<Map<String, Any>>())
    }

    @Test
    fun `should signal a waiting receive task with the event as process variables`() {
        linked(receiveTask(eventTypeFilter = "nl.example.zaak.created"))
        val execution = givenWaitingExecution()
        val result = ObjectMapper().createObjectNode().put("zaakId", "Z-1")

        listener.handle(
            event(
                date = LocalDateTime.of(2026, 10, 20, 12, 0),
                resultType = "zaak",
                resultId = "Z-1",
                result = result,
            ),
        )

        verify(runtimeService).signal(
            execution.id,
            mapOf(
                "cloudEventId" to "event-1",
                "cloudEventType" to "nl.example.zaak.created",
                "cloudEventDate" to "2026-10-20T12:00",
                "cloudEventResultType" to "zaak",
                "cloudEventResultId" to "Z-1",
                "cloudEventData" to result,
            ),
        )
    }

    @Test
    fun `should correlate the message of a waiting intermediate catch event`() {
        linked(
            link(ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, CASE_PROCESS_DEFINITION_ID),
        )
        val execution = givenWaitingExecution()

        listener.handle(event())

        verify(runtimeService).messageEventReceived(
            MESSAGE_NAME,
            execution.id,
            mapOf("cloudEventId" to "event-1", "cloudEventType" to "nl.example.zaak.created"),
        )
    }

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
    fun `should skip a case process whose case definition version is no longer active`() {
        val link = linked(caseProcessStartEvent())
        givenACaseCanBeStartedFor(link, DOCUMENT_ID)
        whenever(caseDefinitionService.getActiveCaseDefinition("cloudevents")).thenReturn(
            CaseDefinition(
                id = CaseDefinitionId("cloudevents", Semver("2.0.0")),
                name = "Cloud events",
                createdDate = null,
                canHaveAssignee = true,
            ),
        )

        listener.handle(event())

        verify(documentService, never()).createDocument(any())
        verify(runtimeService, never()).createMessageCorrelation(any())
    }

    @Test
    fun `should skip a case process whose document cannot be created instead of failing`() {
        val link = linked(caseProcessStartEvent())
        givenACaseCanBeStartedFor(link, DOCUMENT_ID)
        givenStartMessageCorrelates()
        val failed = mock<CreateDocumentResult>()
        doReturn(Optional.empty<Document>()).whenever(failed).resultingDocument()
        whenever(documentService.createDocument(any())).thenReturn(failed)

        listener.handle(event())

        verify(runtimeService, never()).createMessageCorrelation(any())
    }

    @Test
    fun `should not start a process from a message start event without an event type filter`() {
        // Every event in the inbox would start one, the DocumentCreated event of each case it starts included.
        val link = linked(link(ActivityTypeWithEventName.MESSAGE_START_EVENT_START, CASE_PROCESS_DEFINITION_ID))
        givenACaseCanBeStartedFor(link, DOCUMENT_ID)

        listener.handle(event())

        verify(processedCloudEventRepository, never()).saveAndFlush(any())
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

    private fun linked(vararg links: PluginProcessLink): PluginProcessLink {
        whenever(pluginProcessLinkRepository.findByPluginActionDefinitionKey("receive-cloud-event"))
            .thenReturn(links.toList())
        return links.first()
    }

    private fun receiveTask(eventTypeFilter: String? = null): PluginProcessLink =
        link(ActivityTypeWithEventName.RECEIVE_TASK_END, CASE_PROCESS_DEFINITION_ID, eventTypeFilter)

    private fun link(
        activityType: ActivityTypeWithEventName,
        processDefinitionId: String,
        eventTypeFilter: String? = null,
    ): PluginProcessLink {
        val actionProperties = eventTypeFilter?.let { ObjectMapper().createObjectNode().put("eventType", it) }
        return mock<PluginProcessLink>().also {
            whenever(it.id).thenReturn(UUID.randomUUID())
            whenever(it.activityType).thenReturn(activityType)
            whenever(it.activityId).thenReturn("Event_0beitek")
            whenever(it.processDefinitionId).thenReturn(processDefinitionId)
            whenever(it.actionProperties).thenReturn(actionProperties)
        }
    }

    /** One process instance that has entered the linked receive task, or waits at the linked catch event. */
    private fun givenWaitingExecution(): Execution {
        val execution = mock<Execution>()
        whenever(execution.id).thenReturn("execution-1")
        whenever(execution.processInstanceId).thenReturn("process-instance-1")
        val query = mock<ExecutionQuery>()
        whenever(query.processDefinitionId(CASE_PROCESS_DEFINITION_ID)).thenReturn(query)
        whenever(query.activityId("Event_0beitek")).thenReturn(query)
        whenever(query.active()).thenReturn(query)
        whenever(query.list()).thenReturn(listOf(execution))
        whenever(runtimeService.createExecutionQuery()).thenReturn(query)

        val receiveTask = mock<ActivityInstance>()
        whenever(receiveTask.id).thenReturn("activity-instance-1")
        whenever(receiveTask.executionIds).thenReturn(arrayOf("execution-1"))
        val tree = mock<ActivityInstance>()
        whenever(tree.getActivityInstances("Event_0beitek")).thenReturn(arrayOf(receiveTask))
        whenever(runtimeService.getActivityInstance("process-instance-1")).thenReturn(tree)

        val subscription = mock<EventSubscription>()
        whenever(subscription.id).thenReturn("subscription-1")
        whenever(subscription.executionId).thenReturn("execution-1")
        whenever(subscription.processInstanceId).thenReturn("process-instance-1")
        whenever(subscription.eventName).thenReturn(MESSAGE_NAME)
        val subscriptionQuery = mock<EventSubscriptionQuery>()
        whenever(subscriptionQuery.eventType("message")).thenReturn(subscriptionQuery)
        whenever(subscriptionQuery.activityId("Event_0beitek")).thenReturn(subscriptionQuery)
        whenever(subscriptionQuery.eventSubscriptionId("subscription-1")).thenReturn(subscriptionQuery)
        whenever(subscriptionQuery.list()).thenReturn(listOf(subscription))
        whenever(subscriptionQuery.singleResult()).thenReturn(subscription)
        whenever(runtimeService.createEventSubscriptionQuery()).thenReturn(subscriptionQuery)
        val processInstanceQuery = mock<ProcessInstanceQuery>()
        whenever(processInstanceQuery.processInstanceId("process-instance-1")).thenReturn(processInstanceQuery)
        whenever(processInstanceQuery.processDefinitionId(CASE_PROCESS_DEFINITION_ID)).thenReturn(processInstanceQuery)
        whenever(processInstanceQuery.active()).thenReturn(processInstanceQuery)
        whenever(processInstanceQuery.count()).thenReturn(1)
        whenever(runtimeService.createProcessInstanceQuery()).thenReturn(processInstanceQuery)
        return execution
    }

    private fun caseProcessStartEvent(): PluginProcessLink =
        link(ActivityTypeWithEventName.MESSAGE_START_EVENT_START, CASE_PROCESS_DEFINITION_ID, "nl.example.zaak.created")
            .also { givenStartableProcess(CASE_PROCESS_DEFINITION_ID) }

    private fun systemProcessStartEvent(processDefinitionId: String): PluginProcessLink =
        link(ActivityTypeWithEventName.MESSAGE_START_EVENT_START, processDefinitionId, "nl.example.zaak.created").also {
            givenStartableProcess(processDefinitionId)
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

    /** A deployed, active process definition whose linked element is a message start event of the process. */
    private fun givenStartableProcess(processDefinitionId: String) {
        givenMessageOnLinkedElement()
        whenever(repositoryService.getProcessDefinition(processDefinitionId)).thenReturn(mock<ProcessDefinition>())
    }

    /** Stubs the BPMN lookup of the message name on the linked element. */
    private fun givenMessageOnLinkedElement() {
        val message = mock<Message>()
        whenever(message.name).thenReturn(MESSAGE_NAME)
        val messageEventDefinition = mock<MessageEventDefinition>()
        whenever(messageEventDefinition.message).thenReturn(message)
        val catchEvent = mock<CatchEvent>()
        whenever(catchEvent.eventDefinitions).thenReturn(listOf(messageEventDefinition))
        whenever(catchEvent.parentElement).thenReturn(mock<Process>())
        val model = mock<BpmnModelInstance>()
        doReturn(catchEvent).whenever(model).getModelElementById<CatchEvent>(any())
        whenever(repositoryService.getBpmnModelInstance(any())).thenReturn(model)
    }

    /** Stubs the BPMN lookup of the message name and the correlation builder chain. */
    private fun givenStartMessageCorrelates(): MessageCorrelationBuilder {
        givenMessageOnLinkedElement()
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

    private fun event(
        date: LocalDateTime? = null,
        resultType: String? = null,
        resultId: String? = null,
        result: ObjectNode? = null,
    ) = ValtimoEvent(
        id = "event-1",
        type = "nl.example.zaak.created",
        date = date,
        userId = null,
        roles = null,
        resultType = resultType,
        resultId = resultId,
        result = result,
    )

    private companion object {
        private const val CASE_PROCESS_DEFINITION_ID = "Verwerk_cloud_event:1:abc"
        private const val MESSAGE_NAME = "IncomingCloudEvent"
        private const val PROCESS_INSTANCE_ID = "99999999-8888-7777-6666-555555555555"
        private val DOCUMENT_UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")
        private val DOCUMENT_ID = JsonSchemaDocumentId.existingId(DOCUMENT_UUID)
    }
}
