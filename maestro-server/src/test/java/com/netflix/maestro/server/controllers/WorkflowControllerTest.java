package com.netflix.maestro.server.controllers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.db.PropertiesUpdate;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.utils.WorkflowEnrichmentHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.InvalidWorkflowVersionException;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.api.WorkflowOverviewResponse;
import com.netflix.maestro.models.api.WorkflowPropertiesUpdateRequest;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.timeline.WorkflowTimeline;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

public class WorkflowControllerTest extends MaestroBaseTest {

  @Mock private MaestroWorkflowDao mockWorkflowDao;
  @Mock private MaestroWorkflowDeletionDao mockDeletionDao;
  @Mock private User.UserBuilder callerBuilder;
  @Mock private ParamsManager mockParamsManager;
  @Mock private DryRunValidator mockDryRunValidator;
  @Mock private WorkflowEnrichmentHelper workflowEnrichmentHelper;
  private WorkflowController workflowController;

  @Before
  public void before() {
    when(callerBuilder.build()).thenReturn(User.create("unittest_username@netflix.com"));
    when(mockParamsManager.generateMergedWorkflowParams(any(), any()))
        .thenReturn(new LinkedHashMap<>());
    when(mockParamsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenReturn(new LinkedHashMap<>());
    this.workflowController =
        new WorkflowController(
            mockWorkflowDao,
            mockDeletionDao,
            callerBuilder,
            mockDryRunValidator,
            workflowEnrichmentHelper);
  }

  @Test
  public void testAddWorkflow() {
    WorkflowDefinition definition = mock(WorkflowDefinition.class);
    Properties properties = mock(Properties.class);
    Workflow mockWF = mock(Workflow.class);
    when(mockWF.getId()).thenReturn("id");
    when(definition.getWorkflow()).thenReturn(mockWF);
    WorkflowCreateRequest request = mock(WorkflowCreateRequest.class);
    when(request.getProperties()).thenReturn(properties);
    when(request.getWorkflow()).thenReturn(mockWF);
    when(request.getExtraInfo()).thenReturn(Collections.emptyMap());
    when(properties.getOwner()).thenReturn(User.create("tester"));
    when(mockWorkflowDao.addWorkflowDefinition(any(), any())).thenReturn(definition);
    workflowController.addWorkflow(request);
    verify(properties, times(1)).setOwner(any());
    verify(mockWorkflowDao, times(1)).addWorkflowDefinition(any(), any());
    verify(mockDryRunValidator, times(1)).validate(any(), any());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidWorkflow() {
    WorkflowCreateRequest request = mock(WorkflowCreateRequest.class);
    when(request.getWorkflow())
        .thenReturn(Workflow.builder().id("wf-id").steps(new ArrayList<>()).build());
    Properties properties = mock(Properties.class);
    when(request.getProperties()).thenReturn(properties);
    when(mockWorkflowDao.addWorkflowDefinition(any(), any()))
        .thenThrow(new IllegalArgumentException());
    workflowController.addWorkflow(request);
  }

  @Test
  public void testAddInvalidWorkflowWithPropertyTags() throws Exception {
    WorkflowCreateRequest request =
        loadObject("fixtures/api/sample-workflow-create-request.json", WorkflowCreateRequest.class);
    Tag tag = Tag.create("some-tag");
    request.getProperties().setTags(new TagList(Collections.singletonList(tag)));
    AssertHelper.assertThrows(
        "fail to add workflow with property tags",
        MaestroValidationException.class,
        "Cannot set workflow property tags (also known as dynamic tags) for workflow [sample-dag-test-9]. Please use addWorkflowTag endpoint for that.",
        () -> workflowController.addWorkflow(request));
  }

  @Test
  public void testInvalidExtraField() throws Exception {
    WorkflowCreateRequest request =
        loadObject("fixtures/api/sample-workflow-create-request.json", WorkflowCreateRequest.class);
    request.add("workflow_id", "foo");
    AssertHelper.assertThrows(
        "cannot set reserved fields",
        IllegalArgumentException.class,
        "extra info [additional_meta, workflow_id] cannot contain any reserved keys",
        () -> workflowController.addWorkflow(request));
  }

  @Test
  public void testAddWorkflowWhileStillDeleting() {
    WorkflowDefinition definition = mock(WorkflowDefinition.class);
    Properties properties = mock(Properties.class);
    Workflow mockWF = mock(Workflow.class);
    when(mockWF.getId()).thenReturn("id");
    when(definition.getWorkflow()).thenReturn(mockWF);
    WorkflowCreateRequest request = mock(WorkflowCreateRequest.class);
    when(request.getProperties()).thenReturn(properties);
    when(request.getWorkflow()).thenReturn(mockWF);
    when(request.getExtraInfo()).thenReturn(Collections.emptyMap());
    when(properties.getOwner()).thenReturn(User.create("tester"));
    when(mockDeletionDao.isDeletionInProgress(any())).thenReturn(true);

    AssertHelper.assertThrows(
        "cannot add a workflow, whose workflow id is still under deletion",
        MaestroResourceConflictException.class,
        "Cannot push a version for workflow [id] while the system is still deleting",
        () -> workflowController.addWorkflow(request));
  }

  @Test
  public void testUpdateProperties() {
    WorkflowPropertiesUpdateRequest request = mock(WorkflowPropertiesUpdateRequest.class);
    Properties properties = new Properties();
    when(request.getProperties()).thenReturn(properties);
    when(request.isResetRunStrategyRule()).thenReturn(false);
    when(request.isResetWorkflowConcurrency()).thenReturn(false);
    when(request.isResetStepConcurrency()).thenReturn(false);
    workflowController.updateProperties("test-workflow-id", request);
    verify(mockWorkflowDao, times(1))
        .updateWorkflowProperties(
            anyString(), any(User.class), eq(properties), any(PropertiesUpdate.class));
  }

  @Test
  public void testResetProperties() throws Exception {
    WorkflowPropertiesUpdateRequest request =
        loadObject(
            "fixtures/api/sample-workflow-properties-update-request-reset.json",
            WorkflowPropertiesUpdateRequest.class);
    workflowController.updateProperties("test-workflow-id", request);
    verify(mockWorkflowDao, times(1))
        .updateWorkflowProperties(
            anyString(), any(User.class), any(Properties.class), any(PropertiesUpdate.class));
  }

  @Test
  public void testAddWorkflowTag() {
    Tag tagToBeAdded = Tag.create("new-tag");
    tagToBeAdded.addAttribute("description", "test description");
    workflowController.addWorkflowTag("test-workflow-id", tagToBeAdded);
    verify(mockWorkflowDao, times(1))
        .updateWorkflowProperties(
            anyString(),
            any(User.class),
            ArgumentMatchers.argThat(
                properties -> {
                  TagList tagList = properties.getTags();
                  if (tagList.getTags().size() != 1) {
                    return false;
                  }
                  Tag tag = tagList.getTags().getFirst();
                  boolean validTag = tag.getName().equals("new-tag");
                  validTag = validTag && tag.getNamespace().equals(Tag.Namespace.PLATFORM);
                  return validTag;
                }),
            any(PropertiesUpdate.class));
  }

  @Test
  public void testDeleteWorkflowTag() {
    String tagNameToBeDeleted = "new-tag";
    workflowController.deleteWorkflowTag("test-workflow-id", tagNameToBeDeleted);
    verify(mockWorkflowDao, times(1))
        .updateWorkflowProperties(
            anyString(),
            any(User.class),
            ArgumentMatchers.argThat(
                properties -> {
                  TagList tagList = properties.getTags();
                  if (tagList.getTags().size() != 1) {
                    return false;
                  }
                  return tagList.getTags().getFirst().getName().equals("new-tag");
                }),
            any(PropertiesUpdate.class));
  }

  @Test
  public void testInvalidPropertiesChange() {
    when(mockWorkflowDao.updateWorkflowProperties(
            anyString(), any(User.class), any(Properties.class), any(PropertiesUpdate.class)))
        .thenThrow(
            new RuntimeException("BACKEND_ERROR - ERROR: failed to satisfy CHECK constraint"));

    WorkflowPropertiesUpdateRequest request = mock(WorkflowPropertiesUpdateRequest.class);
    when(request.getProperties()).thenReturn(mock(Properties.class));
    when(request.isResetRunStrategyRule()).thenReturn(false);
    when(request.isResetWorkflowConcurrency()).thenReturn(false);
    when(request.isResetStepConcurrency()).thenReturn(false);
    AssertHelper.assertThrows(
        "fail to add a property for non-existing workflow",
        MaestroBadRequestException.class,
        "please check if there exists this workflow",
        () -> workflowController.updateProperties("test-workflow-id", request));
  }

  @Test
  public void testDeleteWorkflow() {
    workflowController.deleteWorkflow("test-workflow");
    verify(mockWorkflowDao, times(1)).deleteWorkflow(anyString(), any());
  }

  @Test
  public void testGetWorkflowVersion() {
    when(mockWorkflowDao.getWorkflowDefinition("test-workflow", "latest"))
        .thenReturn(mock(WorkflowDefinition.class));
    workflowController.getWorkflowVersion("test-workflow", "latest", false);
    verify(mockWorkflowDao, times(1)).getWorkflowDefinition(anyString(), anyString());
  }

  @Test
  public void testEnrichedGetWorkflowVersion() {
    when(mockWorkflowDao.getWorkflowDefinition("test-workflow", "latest"))
        .thenReturn(mock(WorkflowDefinition.class));
    workflowController.getWorkflowVersion("test-workflow", "latest", true);
    verify(mockWorkflowDao, times(1)).getWorkflowDefinition(anyString(), anyString());
    verify(workflowEnrichmentHelper, times(1)).enrichWorkflowDefinition(any());
  }

  @Test
  public void testGetLatestWorkflowPropertiesSnapshot() {
    when(mockWorkflowDao.getWorkflowPropertiesSnapshot("test-workflow", "latest"))
        .thenReturn(
            PropertiesSnapshot.create(
                "test-workflow", 12345L, User.create("tester"), new Properties()));
    workflowController.getWorkflowPropertiesSnapshot("test-workflow", "latest");
    verify(mockWorkflowDao, times(1)).getWorkflowPropertiesSnapshot(anyString(), anyString());
  }

  @Test
  public void testGetWorkflowVersionError() {
    when(mockWorkflowDao.getWorkflowDefinition("test-workflow", "latest"))
        .thenThrow(new InvalidWorkflowVersionException("test-workflow", "latest"));
    AssertHelper.assertThrows(
        "fail to get an workflow version",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version",
        () -> workflowController.getWorkflowVersion("test-workflow", "latest", false));
  }

  @Test
  public void testGetWorkflowOverview() {
    when(mockWorkflowDao.getWorkflowOverview("test-workflow"))
        .thenReturn(mock(WorkflowOverviewResponse.class));
    workflowController.getWorkflowOverview("test-workflow");
    verify(mockWorkflowDao, times(1)).getWorkflowOverview("test-workflow");
  }

  @Test
  public void testGetWorkflowTimeline() {
    when(mockWorkflowDao.getWorkflowTimeline("test-workflow"))
        .thenReturn(mock(WorkflowTimeline.class));
    workflowController.getWorkflowTimeline("test-workflow");
    verify(mockWorkflowDao, times(1)).getWorkflowTimeline("test-workflow");
  }
}
