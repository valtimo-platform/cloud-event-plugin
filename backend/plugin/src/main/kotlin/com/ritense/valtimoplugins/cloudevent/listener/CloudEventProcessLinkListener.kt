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
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.ritense.authorization.AuthorizationContext
import com.ritense.authorization.annotation.RunWithoutAuthorization
import com.ritense.case.service.CaseDefinitionService
import com.ritense.document.domain.impl.request.NewDocumentRequest
import com.ritense.document.service.DocumentService
import com.ritense.inbox.ValtimoEvent
import com.ritense.inbox.ValtimoEventHandler
import com.ritense.plugin.domain.PluginProcessLink
import com.ritense.processdocument.domain.ProcessDefinitionId
import com.ritense.processdocument.domain.impl.OperatonProcessInstanceId
import com.ritense.processdocument.service.ProcessDefinitionCaseDefinitionService
import com.ritense.processdocument.service.ProcessDocumentAssociationService
import com.ritense.processlink.domain.ActivityTypeWithEventName
import com.ritense.processlink.repository.ValtimoPluginProcessLinkRepository
import com.ritense.valtimo.service.ProcessPropertyService
import com.ritense.valtimoplugins.cloudevent.domain.ProcessedCloudEvent
import com.ritense.valtimoplugins.cloudevent.domain.ReceiveCloudEventProperties
import com.ritense.valtimoplugins.cloudevent.repository.ProcessedCloudEventRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.operaton.bpm.engine.RepositoryService
import org.operaton.bpm.engine.RuntimeService
import org.operaton.bpm.engine.runtime.ActivityInstance
import org.operaton.bpm.model.bpmn.instance.CatchEvent
import org.operaton.bpm.model.bpmn.instance.MessageEventDefinition
import org.operaton.bpm.model.bpmn.instance.Process
import org.operaton.bpm.model.xml.instance.ModelElementInstance
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

open class CloudEventProcessLinkListener(
    private val pluginProcessLinkRepository: ValtimoPluginProcessLinkRepository,
    private val runtimeService: RuntimeService,
    private val repositoryService: RepositoryService,
    private val processPropertyService: ProcessPropertyService,
    private val processDefinitionCaseDefinitionService: ProcessDefinitionCaseDefinitionService,
    private val documentService: DocumentService,
    private val processDocumentAssociationService: ProcessDocumentAssociationService,
    private val caseDefinitionService: CaseDefinitionService,
    private val objectMapper: ObjectMapper,
    private val processedCloudEventRepository: ProcessedCloudEventRepository,
) : ValtimoEventHandler {
    private val linksWarnedAboutMissingFilter = ConcurrentHashMap.newKeySet<UUID>()

    @RunWithoutAuthorization
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    override fun handle(event: ValtimoEvent) {
        logger.debug { "Handling incoming cloud event with type '${event.type}'" }

        // Matched first: the inbox carries Valtimo's own events too, which need no deduplication.
        val processLinks =
            pluginProcessLinkRepository
                .findByPluginActionDefinitionKey(ACTION_KEY)
                .filter { matchesFilter(event, it) }
        if (processLinks.isEmpty()) {
            logger.debug { "Cloud event '${event.id}' matched no receive-cloud-event process link" }
            return
        }

        if (processedCloudEventRepository.existsById(event.id)) {
            logger.debug { "Skipping duplicate cloud event '${event.id}'" }
            return
        }
        // Inserted and flushed now, so a concurrent delivery of the same event fails on this row
        // before it acts on anything, rather than after.
        processedCloudEventRepository.saveAndFlush(ProcessedCloudEvent(eventId = event.id))

        val variables = buildProcessVariables(event)
        // What waits at each link is collected before any link acts: an execution this event moves
        // onto another linked activity, or a process it starts, must not receive the same event again.
        val linksWithWaits =
            processLinks.map { processLink ->
                when (processLink.activityType) {
                    ActivityTypeWithEventName.MESSAGE_START_EVENT_START -> processLink to null
                    else -> processLink to findWaits(processLink)
                }
            }
        linksWithWaits.forEach { (processLink, waits) ->
            if (waits == null) {
                startProcessByMessage(processLink, variables)
            } else {
                continueWaits(processLink, waits, variables)
            }
        }
    }

    private fun matchesFilter(
        event: ValtimoEvent,
        processLink: PluginProcessLink,
    ): Boolean {
        val filter =
            processLink.actionProperties
                ?.let { objectMapper.treeToValue(it, ReceiveCloudEventProperties::class.java) }
                ?.eventType
                ?.trim()
                ?.takeUnless { it.isEmpty() }
        // With no filter every event in the inbox would start a process, Valtimo's own included, and
        // with them the DocumentCreated event of each case this starts: a loop without end.
        if (filter == null && processLink.activityType == ActivityTypeWithEventName.MESSAGE_START_EVENT_START) {
            // Once per link: this is checked for every event in the inbox.
            if (linksWarnedAboutMissingFilter.add(processLink.id)) {
                logger.warn {
                    "Skipping process link for '${processLink.processDefinitionId}': a message start event needs an " +
                        "event type filter"
                }
            }
            return false
        }
        return filter == null || filter == event.type
    }

    /**
     * A wait is identified by what the engine keeps while it lasts, not by its execution, whose id
     * changes when a parallel branch ends and its sibling is pruned into the parent: the activity
     * instance of a receive task, or the message subscription of a catch event.
     */
    private data class Wait(
        val processInstanceId: String,
        val id: String,
    )

    /** Suspended process instances are left waiting: the engine refuses to continue them. */
    private fun findWaits(processLink: PluginProcessLink): List<Wait> =
        when (processLink.activityType) {
            // Behind an event-based gateway the execution waits at the gateway, and only its message
            // subscription is on the catch event.
            ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END ->
                runtimeService
                    .createEventSubscriptionQuery()
                    .eventType(MESSAGE_EVENT_TYPE)
                    .activityId(processLink.activityId)
                    .list()
                    // A message start event's subscription has the same type, but no process instance.
                    .filter { it.processInstanceId != null }
                    .filter { subscription ->
                        runtimeService
                            .createProcessInstanceQuery()
                            .processInstanceId(subscription.processInstanceId)
                            .processDefinitionId(processLink.processDefinitionId)
                            .active()
                            .count() > 0
                    }.map { Wait(it.processInstanceId, it.id) }

            else ->
                runtimeService
                    .createExecutionQuery()
                    .processDefinitionId(processLink.processDefinitionId)
                    .activityId(processLink.activityId)
                    .active()
                    .list()
                    .map { it.processInstanceId }
                    .distinct()
                    .flatMap { processInstanceId ->
                        enteredReceiveTasks(processInstanceId, processLink).map { Wait(processInstanceId, it.id) }
                    }
        }

    /**
     * Only a receive task that was entered and not yet left has an activity instance. An execution
     * parked on an asynchronous continuation before or after it has a transition instance instead.
     */
    private fun enteredReceiveTasks(
        processInstanceId: String,
        processLink: PluginProcessLink,
    ): List<ActivityInstance> =
        runtimeService
            .getActivityInstance(processInstanceId)
            ?.getActivityInstances(processLink.activityId)
            ?.toList()
            .orEmpty()

    private fun continueWaits(
        processLink: PluginProcessLink,
        waits: List<Wait>,
        variables: Map<String, Any>,
    ) {
        if (waits.isEmpty()) {
            logger.debug {
                "No waiting executions found for process definition '${processLink.processDefinitionId}' at activity '${processLink.activityId}'"
            }
            return
        }

        waits.forEach { wait ->
            if (!continueWait(processLink, wait, variables)) {
                logger.debug {
                    "Process instance '${wait.processInstanceId}' no longer waits at '${processLink.activityId}'"
                }
            }
        }
    }

    /** False when an earlier link of this event has ended the wait. */
    private fun continueWait(
        processLink: PluginProcessLink,
        wait: Wait,
        variables: Map<String, Any>,
    ): Boolean {
        when (processLink.activityType) {
            ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END -> {
                val subscription =
                    runtimeService
                        .createEventSubscriptionQuery()
                        .eventSubscriptionId(wait.id)
                        .singleResult() ?: return false
                logger.info {
                    "Correlating message to execution '${subscription.executionId}' at '${processLink.activityId}'"
                }
                // The subscription's name has any expression in the BPMN message name resolved.
                runtimeService.messageEventReceived(subscription.eventName, subscription.executionId, variables)
            }

            ActivityTypeWithEventName.RECEIVE_TASK_END -> {
                val executionId =
                    enteredReceiveTasks(wait.processInstanceId, processLink)
                        .firstOrNull { it.id == wait.id }
                        ?.executionIds
                        ?.firstOrNull() ?: return false
                logger.info { "Signaling execution '$executionId' at '${processLink.activityId}'" }
                runtimeService.signal(executionId, variables)
            }

            else -> error("Unsupported activity type '${processLink.activityType}' for cloud event process link")
        }
        return true
    }

    private fun startProcessByMessage(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        // An event subprocess's start event has the same activity type, but no process can be started there.
        val startEvent =
            repositoryService
                .getBpmnModelInstance(processLink.processDefinitionId)
                .getModelElementById<ModelElementInstance>(processLink.activityId)
        if (startEvent?.parentElement !is Process) {
            logger.warn {
                "Skipping process link for '${processLink.processDefinitionId}': " +
                    "'${processLink.activityId}' is not a start event of the process itself"
            }
            return
        }

        if (repositoryService.getProcessDefinition(processLink.processDefinitionId).isSuspended) {
            logger.warn {
                "Skipping process link for '${processLink.processDefinitionId}': the process definition is suspended"
            }
            return
        }

        if (processPropertyService.isSystemProcessById(processLink.processDefinitionId)) {
            startSystemProcessByMessage(processLink, variables)
        } else {
            startDocumentProcessByMessage(processLink, variables)
        }
    }

    /**
     * Starts only the latest version of a system process.
     *
     * Every edit of a system process deploys a new version, and Valtimo copies the process links
     * onto it while the old version keeps its own. Without this, one event would start one
     * instance per version ever deployed. Operaton itself only keeps message start subscriptions
     * for the latest version, so this is what a message start event means there too.
     *
     * Case processes need no such check: redeploying a draft detaches the previous definition
     * from its case definition, which [startDocumentProcessByMessage] already skips.
     */
    private fun startSystemProcessByMessage(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        if (!isLatestVersion(processLink.processDefinitionId)) {
            logger.debug {
                "Skipping process link for '${processLink.processDefinitionId}': a newer version of the process " +
                    "is deployed, and only the latest version is started by message"
            }
            return
        }

        val messageName = getMessageName(processLink) ?: return
        logger.info {
            "Starting system process instance by message '$messageName' for process definition '${processLink.processDefinitionId}'"
        }
        runtimeService
            .createMessageCorrelation(messageName)
            .processDefinitionId(processLink.processDefinitionId)
            .setVariables(variables)
            .correlateStartMessage()
    }

    private fun startDocumentProcessByMessage(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        val processDefinitionCaseDefinition =
            processDefinitionCaseDefinitionService
                .findByProcessDefinitionIdOrNull(ProcessDefinitionId(processLink.processDefinitionId))
                ?: run {
                    logger.debug {
                        "Skipping process link for '${processLink.processDefinitionId}': no case definition is " +
                            "linked to it, typically because it is an earlier, detached version of the process"
                    }
                    return
                }

        // Only the deployed, active version of a case definition may be started. Without
        // this an old process link would keep creating cases against a superseded version.
        val activeCaseDefinition =
            caseDefinitionService.getActiveCaseDefinition(processDefinitionCaseDefinition.id.caseDefinitionId.key)
        if (activeCaseDefinition?.id != processDefinitionCaseDefinition.id.caseDefinitionId) {
            logger.debug {
                "Skipping process link for '${processLink.processDefinitionId}': it points at a case definition " +
                    "version that is no longer active"
            }
            return
        }

        // Skipped rather than thrown. This is configuration, so an exception would fail the
        // event, and every other link it matched, on every delivery attempt.
        if (!processDefinitionCaseDefinition.canInitializeDocument) {
            logger.warn {
                "Not starting a case for process definition '${processLink.processDefinitionId}': " +
                    "canInitializeDocument is false on the linked case definition"
            }
            return
        }

        val messageName = getMessageName(processLink) ?: return
        logger.info {
            "Creating a case for case definition '${activeCaseDefinition.id.key}' " +
                "(${activeCaseDefinition.id.versionTag}) by correlating start message '$messageName' to " +
                "'${processLink.activityId}'"
        }

        val newDocumentRequest =
            NewDocumentRequest(
                activeCaseDefinition.id.key,
                activeCaseDefinition.id.key,
                activeCaseDefinition.id.versionTag.toString(),
                JsonNodeFactory.instance.objectNode(),
            )
        val documentResult =
            AuthorizationContext.runWithoutAuthorization {
                documentService.createDocument(newDocumentRequest)
            }
        val document =
            documentResult.resultingDocument().orElse(null)
                ?: run {
                    // Typically a document schema with required fields, which an empty document cannot meet.
                    logger.warn {
                        "Not starting a case for process definition '${processLink.processDefinitionId}': " +
                            "its document could not be created: ${documentResult.errors()}"
                    }
                    return
                }

        // Correlated to the start message rather than started by process definition key.
        // ProcessDocumentService.newDocumentAndStartProcess, the obvious alternative, starts a
        // process by key, and Operaton then enters it at whichever start event it considers the
        // process's initial activity. A process with both a plain start event (someone fills in
        // the start form) and this message start event is a normal shape, and on that shape "by
        // key" silently lands on the plain one: the event's own start event never runs, so
        // neither do its execution listeners, and the case is created without any of the cloud
        // event on it.
        val processInstance =
            runtimeService
                .createMessageCorrelation(messageName)
                .processDefinitionId(processLink.processDefinitionId)
                // Valtimo resolves `doc:` for a process instance from its business key, and
                // associates the instance with that document when its start event runs. Both
                // depend on this being the document id. Valtimo's own start path sets it the
                // same way.
                .processInstanceBusinessKey(document.id().toString())
                .setVariables(variables)
                .correlateStartMessage()

        // Usually already associated by Valtimo's start event listener, and possibly with
        // another document: a building block's start listener points the association at its
        // own document. Creating one regardless would throw on that, so only fill the gap, the
        // same way Valtimo's own start paths do.
        AuthorizationContext.runWithoutAuthorization {
            if (processDocumentAssociationService
                    .findProcessDocumentInstance(OperatonProcessInstanceId(processInstance.id))
                    .isEmpty
            ) {
                processDocumentAssociationService.createProcessDocumentInstance(
                    processInstance.id,
                    UUID.fromString(document.id().toString()),
                    repositoryService.getProcessDefinition(processLink.processDefinitionId).let { it.name ?: it.key },
                )
            }
        }
    }

    private fun isLatestVersion(processDefinitionId: String): Boolean {
        val key = repositoryService.getProcessDefinition(processDefinitionId).key
        // A list rather than a single result: with tenants, there is a latest version per tenant.
        return repositoryService
            .createProcessDefinitionQuery()
            .processDefinitionKey(key)
            .latestVersion()
            .list()
            .any { it.id == processDefinitionId }
    }

    private fun buildProcessVariables(event: ValtimoEvent): Map<String, Any> {
        val variables =
            mutableMapOf<String, Any>(
                "cloudEventId" to event.id,
                "cloudEventType" to event.type,
            )
        event.date?.toString()?.let { variables["cloudEventDate"] = it }
        event.resultType?.let { variables["cloudEventResultType"] = it }
        event.resultId?.let { variables["cloudEventResultId"] = it }
        event.result?.let { variables["cloudEventData"] = it }
        return variables
    }

    /** Null, with a warning, when the linked element has no message: configuration is skipped, not thrown. */
    private fun getMessageName(processLink: PluginProcessLink): String? {
        val element =
            repositoryService
                .getBpmnModelInstance(processLink.processDefinitionId)
                .getModelElementById<CatchEvent>(processLink.activityId)
        val messageName =
            element
                ?.eventDefinitions
                ?.filterIsInstance<MessageEventDefinition>()
                ?.firstOrNull()
                ?.message
                ?.name
        if (messageName == null) {
            logger.warn {
                "Skipping process link for '${processLink.processDefinitionId}': element " +
                    "'${processLink.activityId}' is not a catch event with a message"
            }
        }
        return messageName
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private const val ACTION_KEY = "receive-cloud-event"
        private const val MESSAGE_EVENT_TYPE = "message"
    }
}
