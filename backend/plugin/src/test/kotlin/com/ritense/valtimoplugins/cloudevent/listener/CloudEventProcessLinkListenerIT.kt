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
import com.ritense.authorization.AuthorizationContext
import com.ritense.document.domain.impl.JsonSchemaDocumentId
import com.ritense.document.service.DocumentService
import com.ritense.inbox.ValtimoEvent
import com.ritense.plugin.domain.PluginProcessLink
import com.ritense.plugin.service.PluginService
import com.ritense.processdocument.domain.impl.OperatonProcessInstanceId
import com.ritense.processdocument.service.ProcessDocumentAssociationService
import com.ritense.processlink.domain.ActivityTypeWithEventName
import com.ritense.processlink.repository.ValtimoPluginProcessLinkRepository
import com.ritense.valtimoplugins.cloudevent.BaseIntegrationTest
import com.ritense.valtimoplugins.cloudevent.domain.ProcessedCloudEvent
import com.ritense.valtimoplugins.cloudevent.repository.ProcessedCloudEventRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.operaton.bpm.engine.HistoryService
import org.operaton.bpm.engine.ManagementService
import org.operaton.bpm.engine.RepositoryService
import org.operaton.bpm.engine.RuntimeService
import org.operaton.bpm.model.bpmn.Bpmn
import org.operaton.bpm.model.bpmn.BpmnModelInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DataIntegrityViolationException
import java.util.UUID

internal class CloudEventProcessLinkListenerIT : BaseIntegrationTest() {
    @Autowired
    lateinit var listener: CloudEventProcessLinkListener

    @Autowired
    lateinit var processedCloudEventRepository: ProcessedCloudEventRepository

    @Autowired
    lateinit var pluginService: PluginService

    @Autowired
    lateinit var pluginProcessLinkRepository: ValtimoPluginProcessLinkRepository

    @Autowired
    lateinit var repositoryService: RepositoryService

    @Autowired
    lateinit var documentService: DocumentService

    @Autowired
    lateinit var processDocumentAssociationService: ProcessDocumentAssociationService

    @Autowired
    lateinit var runtimeService: RuntimeService

    @Autowired
    lateinit var managementService: ManagementService

    @Autowired
    lateinit var historyService: HistoryService

    // Unique per test: the links a test saves outlive it, and must not pick up another test's events.
    private val testRun = UUID.randomUUID()
    private val receivedType = "nl.example.it.received.$testRun"
    private val closedType = "nl.example.it.closed.$testRun"
    private val startedType = "nl.example.it.started.$testRun"
    private val caseStartedType = "nl.example.it.case-started.$testRun"

    @Test
    fun `should act on an event once, however often it is delivered`() {
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess("cloud-event-it-${UUID.randomUUID()}")
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .receiveTask("WaitForEvent")
                    .receiveTask("WaitForNextEvent")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        link(processDefinitionId, "WaitForNextEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id
        val received = event(type = receivedType)
        val processedBefore = processedCloudEventRepository.count()

        // One event continues the execution once, though the activity it moves on to is linked too.
        listener.handle(received)
        assertEquals(listOf("WaitForNextEvent"), runtimeService.getActiveActivityIds(processInstanceId))

        listener.handle(received)
        assertEquals(listOf("WaitForNextEvent"), runtimeService.getActiveActivityIds(processInstanceId))
        assertTrue(processedCloudEventRepository.existsById(received.id))
        assertEquals(processedBefore + 1, processedCloudEventRepository.count())

        listener.handle(event(type = receivedType))
        assertNull(runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult())
    }

    @Test
    fun `should refuse to record an event that another delivery recorded first`() {
        // A merge would quietly update the row, and a concurrent duplicate would then act a second time.
        val eventId = UUID.randomUUID().toString()
        processedCloudEventRepository.saveAndFlush(ProcessedCloudEvent(eventId = eventId))

        assertThrows<DataIntegrityViolationException> {
            processedCloudEventRepository.saveAndFlush(ProcessedCloudEvent(eventId = eventId))
        }
    }

    @Test
    fun `should continue a waiting receive task and then a waiting message catch event`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .receiveTask("WaitForEvent")
                    .intermediateCatchEvent("WaitForMessage")
                    .message("CloudEventIt$key")
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        link(processDefinitionId, "WaitForMessage", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = "nl.example.it.other"))
        assertEquals(listOf("WaitForEvent"), runtimeService.getActiveActivityIds(processInstanceId))

        val received = event(type = receivedType, result = objectMapper.createObjectNode().put("zaakId", "Z-1"))
        listener.handle(received)
        assertEquals(listOf("WaitForMessage"), runtimeService.getActiveActivityIds(processInstanceId))
        assertEquals(received.result, runtimeService.getVariable(processInstanceId, "cloudEventData"))
        assertEquals(received.id, runtimeService.getVariable(processInstanceId, "cloudEventId"))
        assertEquals(receivedType, runtimeService.getVariable(processInstanceId, "cloudEventType"))

        val closed = event(type = closedType)
        listener.handle(closed)
        assertEquals(listOf("Done"), runtimeService.getActiveActivityIds(processInstanceId))
        assertEquals(closed.id, runtimeService.getVariable(processInstanceId, "cloudEventId"))

        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    @Test
    fun `should leave a suspended process instance waiting and continue the others`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .receiveTask("WaitForEvent")
                    .intermediateCatchEvent("WaitForMessage")
                    .message("CloudEventIt$key")
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        link(processDefinitionId, "WaitForMessage", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val suspended = runtimeService.startProcessInstanceById(processDefinitionId).id
        val active = runtimeService.startProcessInstanceById(processDefinitionId).id
        runtimeService.suspendProcessInstanceById(suspended)

        listener.handle(event(type = receivedType))
        assertEquals(listOf("WaitForEvent"), runtimeService.getActiveActivityIds(suspended))
        assertEquals(listOf("WaitForMessage"), runtimeService.getActiveActivityIds(active))

        val alsoActive = runtimeService.startProcessInstanceById(processDefinitionId).id
        listener.handle(event(type = receivedType))
        runtimeService.suspendProcessInstanceById(active)

        listener.handle(event(type = closedType))
        assertEquals(listOf("WaitForMessage"), runtimeService.getActiveActivityIds(active))
        assertEquals(listOf("Done"), runtimeService.getActiveActivityIds(alsoActive))

        listOf(suspended, active, alsoActive).forEach { runtimeService.deleteProcessInstance(it, "test finished") }
    }

    @Test
    fun `should correlate a catch event whose message name is an expression`() {
        // The engine resolves the expression when it subscribes the waiting execution.
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .intermediateCatchEvent("WaitForMessage")
                    .message("CloudEventIt-\${zaakId}")
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForMessage", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val processInstanceId =
            runtimeService
                .startProcessInstanceById(
                    processDefinitionId,
                    mapOf("zaakId" to "Z-1"),
                ).id

        listener.handle(event(type = closedType))

        assertEquals(listOf("Done"), runtimeService.getActiveActivityIds(processInstanceId))
        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    @Test
    fun `should not mistake the message start event of another process for a waiting catch event`() {
        // A message start subscription has the same activity id here, but belongs to no process instance.
        val key = "cloud-event-it-${UUID.randomUUID()}"
        deploy(
            Bpmn
                .createExecutableProcess("$key-other")
                .operatonHistoryTimeToLive(1)
                .startEvent("WaitForMessage")
                .message("CloudEventItOther$key")
                .endEvent()
                .done(),
        )
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .intermediateCatchEvent("WaitForMessage")
                    .message("CloudEventIt$key")
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForMessage", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = closedType))

        assertEquals(listOf("Done"), runtimeService.getActiveActivityIds(processInstanceId))
        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    @Test
    fun `should correlate the message of a catch event that waits behind an event-based gateway`() {
        // The execution waits at the gateway; only its message subscription is on the catch event.
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .eventBasedGateway()
                    .id("WaitForMessageOrTimeout")
                    .intermediateCatchEvent("WaitForMessage")
                    .message("CloudEventIt$key")
                    .receiveTask("Done")
                    .endEvent()
                    .moveToLastGateway()
                    .intermediateCatchEvent("Timeout")
                    .timerWithDuration("PT1H")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForMessage", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id
        assertEquals(listOf("WaitForMessageOrTimeout"), runtimeService.getActiveActivityIds(processInstanceId))

        listener.handle(event(type = closedType))

        assertEquals(listOf("Done"), runtimeService.getActiveActivityIds(processInstanceId))
        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    @Test
    fun `should skip a waiting execution that an earlier link of the same event ended`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(Bpmn.readModelFromStream(processEndedByEitherOfTwoParallelWaits(key).byteInputStream()))
        link(processDefinitionId, "WaitHere", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        link(processDefinitionId, "OrWaitThere", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = receivedType))

        assertNull(runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult())
    }

    @Test
    fun `should continue both of two parallel waits when the first one ends its branch`() {
        // Ending one branch prunes the other branch's execution into its parent, under a new execution id.
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .parallelGateway("Fork")
                    .receiveTask("WaitHere")
                    .endEvent()
                    .moveToNode("Fork")
                    .receiveTask("OrWaitThere")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitHere", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        link(processDefinitionId, "OrWaitThere", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = receivedType))

        assertNull(runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult())
    }

    @Test
    fun `should continue both of two parallel message catch events when the first one ends its branch`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .parallelGateway("Fork")
                    .intermediateCatchEvent("WaitHere")
                    .message("CloudEventItHere$key")
                    .endEvent()
                    .moveToNode("Fork")
                    .intermediateCatchEvent("OrWaitThere")
                    .message("CloudEventItThere$key")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitHere", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        link(processDefinitionId, "OrWaitThere", ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END, closedType)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = closedType))

        assertNull(runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult())
    }

    @Test
    fun `should not signal a receive task that its asynchronous continuation has not entered yet`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .receiveTask("WaitForEvent")
                    .operatonAsyncBefore()
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        managementService.suspendJobDefinitionByProcessDefinitionId(processDefinitionId, true)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = receivedType))
        assertEquals(0, runtimeService.getActivityInstance(processInstanceId).getActivityInstances("Done").size)

        runAsynchronousContinuation(processInstanceId)
        listener.handle(event(type = receivedType))
        assertEquals(1, runtimeService.getActivityInstance(processInstanceId).getActivityInstances("Done").size)

        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    @Test
    fun `should not signal a receive task again while its asynchronous continuation is pending`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(
                Bpmn
                    .createExecutableProcess(key)
                    .operatonHistoryTimeToLive(1)
                    .startEvent()
                    .receiveTask("WaitForEvent")
                    .operatonAsyncAfter()
                    .receiveTask("Done")
                    .endEvent()
                    .done(),
            )
        link(processDefinitionId, "WaitForEvent", ActivityTypeWithEventName.RECEIVE_TASK_END, receivedType)
        managementService.suspendJobDefinitionByProcessDefinitionId(processDefinitionId, true)
        val processInstanceId = runtimeService.startProcessInstanceById(processDefinitionId).id

        listener.handle(event(type = receivedType))
        listener.handle(event(type = receivedType))
        runAsynchronousContinuation(processInstanceId)

        assertEquals(1, runtimeService.getActivityInstance(processInstanceId).getActivityInstances("Done").size)
        assertEquals(
            1,
            historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(
                    processInstanceId,
                ).activityId("WaitForEvent")
                .count(),
        )

        runtimeService.deleteProcessInstance(processInstanceId, "test finished")
    }

    private fun runAsynchronousContinuation(processInstanceId: String) {
        val job = managementService.createJobQuery().processInstanceId(processInstanceId).singleResult()
        managementService.activateJobById(job.id)
        managementService.executeJob(job.id)
    }

    /** Either branch ends the whole process at its terminate end event, and with it the other branch. */
    private fun processEndedByEitherOfTwoParallelWaits(key: String) =
        """
        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="cloud-event-it">
          <process id="$key" isExecutable="true" camunda:historyTimeToLive="1">
            <startEvent id="Start"/>
            <sequenceFlow id="ToFork" sourceRef="Start" targetRef="Fork"/>
            <parallelGateway id="Fork"/>
            <sequenceFlow id="ToWaitHere" sourceRef="Fork" targetRef="WaitHere"/>
            <sequenceFlow id="ToOrWaitThere" sourceRef="Fork" targetRef="OrWaitThere"/>
            <receiveTask id="WaitHere"/>
            <receiveTask id="OrWaitThere"/>
            <sequenceFlow id="FromWaitHere" sourceRef="WaitHere" targetRef="TerminateHere"/>
            <sequenceFlow id="FromOrWaitThere" sourceRef="OrWaitThere" targetRef="TerminateThere"/>
            <endEvent id="TerminateHere"><terminateEventDefinition/></endEvent>
            <endEvent id="TerminateThere"><terminateEventDefinition/></endEvent>
          </process>
        </definitions>
        """.trimIndent()

    @Test
    fun `should start the latest version of a system process at its message start event`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val earlier = deploy(Bpmn.readModelFromStream(systemProcessStartedByMessage(key).byteInputStream()))
        val latest = deploy(Bpmn.readModelFromStream(systemProcessStartedByMessage(key).byteInputStream()))
        link(earlier, "StartByCloudEvent", ActivityTypeWithEventName.MESSAGE_START_EVENT_START, startedType)
        link(latest, "StartByCloudEvent", ActivityTypeWithEventName.MESSAGE_START_EVENT_START, startedType)
        val started = event(type = startedType)

        listener.handle(started)

        val instances = runtimeService.createProcessInstanceQuery().processDefinitionKey(key).list()
        assertEquals(listOf(latest), instances.map { it.processDefinitionId })
        assertEquals(listOf("Started"), runtimeService.getActiveActivityIds(instances.single().id))
        assertEquals(started.id, runtimeService.getVariable(instances.single().id, "cloudEventId"))

        runtimeService.deleteProcessInstance(instances.single().id, "test finished")
    }

    @Test
    fun `should create a case started at the linked message start event of its process`() {
        // The case process from src/test/resources/config/case/cloud-event-it, imported at startup.
        val processDefinitionId =
            repositoryService
                .createProcessDefinitionQuery()
                .processDefinitionKey("cloud-event-it-case")
                .latestVersion()
                .singleResult()
                .id
        link(
            processDefinitionId,
            "StartByCloudEvent",
            ActivityTypeWithEventName.MESSAGE_START_EVENT_START,
            caseStartedType,
        )
        val started = event(type = caseStartedType)

        listener.handle(started)

        val instance =
            runtimeService
                .createProcessInstanceQuery()
                .processDefinitionId(processDefinitionId)
                .variableValueEquals("cloudEventId", started.id)
                .singleResult()
        assertEquals(listOf("StartedByCloudEvent"), runtimeService.getActiveActivityIds(instance.id))
        AuthorizationContext.runWithoutAuthorization {
            val document =
                documentService.findBy(
                    JsonSchemaDocumentId.existingId(UUID.fromString(instance.businessKey)),
                )
            assertEquals("cloud-event-it", document.orElseThrow().definitionId().name())
            val association =
                processDocumentAssociationService.findProcessDocumentInstance(OperatonProcessInstanceId(instance.id))
            assertEquals(
                instance.businessKey,
                association
                    .orElseThrow()
                    .processDocumentInstanceId()
                    .documentId()
                    .toString(),
            )
        }

        runtimeService.deleteProcessInstance(instance.id, "test finished")
    }

    @Test
    fun `should skip a link on the message start event of an event subprocess instead of failing`() {
        val key = "cloud-event-it-${UUID.randomUUID()}"
        val processDefinitionId =
            deploy(Bpmn.readModelFromStream(systemProcessWithMessageEventSubprocess(key).byteInputStream()))
        link(
            processDefinitionId,
            "StartSubprocessByCloudEvent",
            ActivityTypeWithEventName.MESSAGE_START_EVENT_START,
            startedType,
        )

        listener.handle(event(type = startedType))

        assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionKey(key).count())
    }

    private fun systemProcessWithMessageEventSubprocess(key: String) =
        """
        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="cloud-event-it">
          <message id="SubprocessMessage" name="CloudEventItSubprocess$key"/>
          <process id="$key" isExecutable="true" camunda:historyTimeToLive="1">
            <extensionElements>
              <camunda:properties>
                <camunda:property name="systemProcess" value="true"/>
              </camunda:properties>
            </extensionElements>
            <startEvent id="Start"/>
            <sequenceFlow id="ToWait" sourceRef="Start" targetRef="Wait"/>
            <receiveTask id="Wait"/>
            <subProcess id="OnCloudEvent" triggeredByEvent="true">
              <startEvent id="StartSubprocessByCloudEvent" isInterrupting="false">
                <messageEventDefinition messageRef="SubprocessMessage"/>
              </startEvent>
              <sequenceFlow id="ToHandled" sourceRef="StartSubprocessByCloudEvent" targetRef="Handled"/>
              <endEvent id="Handled"/>
            </subProcess>
          </process>
        </definitions>
        """.trimIndent()

    /** Valtimo reads `systemProcess` from the process's properties when it is deployed. */
    private fun systemProcessStartedByMessage(key: String) =
        """
        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="cloud-event-it">
          <message id="StartMessage" name="CloudEventIt$key"/>
          <process id="$key" isExecutable="true" camunda:historyTimeToLive="1">
            <extensionElements>
              <camunda:properties>
                <camunda:property name="systemProcess" value="true"/>
              </camunda:properties>
            </extensionElements>
            <startEvent id="StartByCloudEvent">
              <messageEventDefinition messageRef="StartMessage"/>
            </startEvent>
            <sequenceFlow id="ToStarted" sourceRef="StartByCloudEvent" targetRef="Started"/>
            <receiveTask id="Started"/>
          </process>
        </definitions>
        """.trimIndent()

    private fun deploy(model: BpmnModelInstance): String {
        val deployment =
            repositoryService
                .createDeployment()
                .addModelInstance("${UUID.randomUUID()}.bpmn", model)
                .deploy()
        return repositoryService
            .createProcessDefinitionQuery()
            .deploymentId(deployment.id)
            .singleResult()
            .id
    }

    private fun link(
        processDefinitionId: String,
        activityId: String,
        activityType: ActivityTypeWithEventName,
        eventType: String,
    ) {
        val configuration =
            AuthorizationContext.runWithoutAuthorization {
                pluginService.createPluginConfiguration("Cloud event", objectMapper.createObjectNode(), "cloud-event")
            }
        pluginProcessLinkRepository.save(
            PluginProcessLink(
                id = UUID.randomUUID(),
                processDefinitionId = processDefinitionId,
                activityId = activityId,
                activityType = activityType,
                actionProperties = objectMapper.createObjectNode().put("eventType", eventType),
                pluginConfigurationId = configuration.id,
                pluginActionDefinitionKey = "receive-cloud-event",
            ),
        )
    }

    private fun event(
        type: String,
        result: ObjectNode? = null,
    ) = ValtimoEvent(
        id = UUID.randomUUID().toString(),
        type = type,
        date = null,
        userId = null,
        roles = null,
        resultType = null,
        resultId = null,
        result = result,
    )

    private companion object {
        private val objectMapper = ObjectMapper()
    }
}
