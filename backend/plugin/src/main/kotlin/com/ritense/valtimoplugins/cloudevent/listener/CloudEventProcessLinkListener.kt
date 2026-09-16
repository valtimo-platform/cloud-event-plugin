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
import org.operaton.bpm.engine.runtime.Execution
import org.operaton.bpm.model.bpmn.instance.CatchEvent
import org.operaton.bpm.model.bpmn.instance.MessageEventDefinition
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

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
    @RunWithoutAuthorization
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    override fun handle(event: ValtimoEvent) {
        logger.debug { "Handling incoming cloud event with type '${event.type}'" }

        if (processedCloudEventRepository.existsById(event.id)) {
            logger.debug { "Skipping duplicate cloud event '${event.id}'" }
            return
        }
        processedCloudEventRepository.save(ProcessedCloudEvent(eventId = event.id))

        val processLinks: List<PluginProcessLink> =
            pluginProcessLinkRepository
                .findByPluginActionDefinitionKey(ACTION_KEY)
        // .findByPluginDefinitionKeyAndPluginActionDefinitionKey(PLUGIN_KEY, ACTION_KEY)
        if (processLinks.isEmpty()) {
            logger.debug { "Cloud event '${event.id}' matched no receive-cloud-event process link" }
            return
        }

        val variables = buildProcessVariables(event)
        processLinks.forEach { processLink ->
            if (matchesFilter(event, processLink)) {
                when (processLink.activityType) {
                    ActivityTypeWithEventName.MESSAGE_START_EVENT_START -> startProcessByMessage(processLink, variables)
                    else -> signalWaitingExecutions(processLink, variables)
                }
            }
        }
    }

    private fun matchesFilter(
        event: ValtimoEvent,
        processLink: PluginProcessLink,
    ): Boolean {
        val properties = processLink.actionProperties ?: return true
        val filter = objectMapper.treeToValue(properties, ReceiveCloudEventProperties::class.java)
        return !(!filter.eventType.isNullOrBlank() && filter.eventType != event.type)
    }

    private fun signalWaitingExecutions(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        val executions =
            runtimeService
                .createExecutionQuery()
                .processDefinitionId(processLink.processDefinitionId)
                .activityId(processLink.activityId)
                .list()

        if (executions.isEmpty()) {
            logger.debug {
                "No waiting executions found for process definition '${processLink.processDefinitionId}' at activity '${processLink.activityId}'"
            }
            return
        }

        executions.forEach { execution ->
            logger.info {
                "Signaling execution '${execution.id}' in process instance '${execution.processInstanceId}' at activity '${processLink.activityId}'"
            }
            continueExecution(execution, processLink, variables)
        }
    }

    private fun continueExecution(
        execution: Execution,
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        when (processLink.activityType) {
            ActivityTypeWithEventName.RECEIVE_TASK_END ->
                runtimeService.signal(execution.id, variables)

            ActivityTypeWithEventName.INTERMEDIATE_CATCH_EVENT_END -> {
                val messageName = getMessageName(processLink)
                runtimeService.messageEventReceived(messageName, execution.id, variables)
            }

            else -> error("Unsupported activity type '${processLink.activityType}' for cloud event process link")
        }
    }

    private fun startProcessByMessage(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        if (processPropertyService.isSystemProcessById(processLink.processDefinitionId)) {
            startSystemProcessByMessage(processLink, variables)
        } else {
            startDocumentProcessByMessage(processLink, variables)
        }
    }

    private fun startSystemProcessByMessage(
        processLink: PluginProcessLink,
        variables: Map<String, Any>,
    ) {
        val messageName = getMessageName(processLink)
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
            try {
                processDefinitionCaseDefinitionService
                    .findByProcessDefinitionId(ProcessDefinitionId(processLink.processDefinitionId))
                    ?: run {
                        logger.warn {
                            "No case definition linked to process definition '${processLink.processDefinitionId}'"
                        }
                        return
                    }
            } catch (e: Exception) {
                logger.warn(e) {
                    "No case definition linked to process definition '${processLink.processDefinitionId}'"
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

        require(processDefinitionCaseDefinition.canInitializeDocument) {
            "Cannot start process for process definition '${processLink.processDefinitionId}' " +
                "because canInitializeDocument is false on the linked case definition."
        }

        val messageName = getMessageName(processLink)
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
                ?: error("Failed to create a case for the incoming cloud event: ${documentResult.errors()}")

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
                // Valtimo resolves `doc:` for a process instance through the process-document
                // association, and falls back to the business key while that association does
                // not exist yet. It cannot exist yet here - it is created below, once the
                // instance has an id - so the start event's own listeners depend on this
                // business key to reach the document. Valtimo's own start path sets it the
                // same way.
                .processInstanceBusinessKey(document.id().toString())
                .setVariables(variables)
                .correlateStartMessage()

        AuthorizationContext.runWithoutAuthorization {
            processDocumentAssociationService.createProcessDocumentInstance(
                processInstance.id,
                UUID.fromString(document.id().toString()),
                repositoryService.getProcessDefinition(processLink.processDefinitionId).name,
            )
        }
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

    private fun getMessageName(processLink: PluginProcessLink): String {
        val model = repositoryService.getBpmnModelInstance(processLink.processDefinitionId)
        val element =
            model.getModelElementById<CatchEvent>(processLink.activityId)
                ?: error(
                    "No catch event '${processLink.activityId}' in process definition " +
                        "'${processLink.processDefinitionId}'",
                )
        return element.eventDefinitions
            .filterIsInstance<MessageEventDefinition>()
            .firstOrNull()
            ?.message
            ?.name
            ?: error(
                "No message event definition on element '${processLink.activityId}' in process definition " +
                    "'${processLink.processDefinitionId}'",
            )
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private const val PLUGIN_KEY = "cloud-event"
        private const val ACTION_KEY = "receive-cloud-event"
    }
}
