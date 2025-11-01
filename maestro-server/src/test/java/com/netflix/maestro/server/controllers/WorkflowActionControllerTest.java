package com.netflix.maestro.server.controllers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.api.WorkflowStartRequest;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WorkflowActionControllerTest extends MaestroBaseTest {
  @Mock private WorkflowActionHandler actionHandler;
  @Mock private User.UserBuilder callerBuilder;
  private WorkflowActionController actionController;

  @Before
  public void setUp() {
    when(callerBuilder.build()).thenReturn(User.create("unittest_username@netflix.com"));
    actionController = new WorkflowActionController(actionHandler, callerBuilder);
  }

  @Test
  public void testStartWorkflowInstance() {
    WorkflowStartRequest startRequest = mock(WorkflowStartRequest.class);
    Initiator mockInitiator = mock(ManualInitiator.class);
    when(startRequest.getInitiator()).thenReturn(mockInitiator);
    RunResponse runResponse = mock(RunResponse.class);
    when(actionHandler.start(any(), any(), any())).thenReturn(runResponse);
    actionController.startWorkflowInstance("foo", "1", startRequest);
    verify(actionHandler, times(1)).start(eq("foo"), eq("1"), any(RunRequest.class));
    verify(runResponse, times(1)).toWorkflowStartResponse();
  }

  @Test
  public void testValidateWorkflow() {
    WorkflowCreateRequest request = mock(WorkflowCreateRequest.class);
    Workflow mockWF = mock(Workflow.class);
    when(mockWF.getId()).thenReturn("id");
    when(request.getWorkflow()).thenReturn(mockWF);
    actionController.validateWorkflow(request);
    verify(actionHandler, times(1)).validate(request, callerBuilder.build());
  }

  @Test
  public void testValidateWorkflowYaml() {
    WorkflowCreateRequest request = mock(WorkflowCreateRequest.class);
    Workflow mockWF = mock(Workflow.class);
    when(mockWF.getId()).thenReturn("id");
    when(request.getWorkflow()).thenReturn(mockWF);
    actionController.validateWorkflowYaml(request);
    verify(actionHandler, times(1)).validate(request, callerBuilder.build());
  }

  @Test
  public void testDoWorkflowAction() {
    actionController.deactivateWorkflow("test-workflow");
    verify(actionHandler, times(1)).deactivate(eq("test-workflow"), any());
    actionController.stopWorkflow("test-workflow");
    verify(actionHandler, times(1)).stop(eq("test-workflow"), any());
    actionController.killWorkflow("test-workflow");
    verify(actionHandler, times(1)).kill(eq("test-workflow"), any());
  }

  @Test
  public void testDoWorkflowVersionAction() {
    actionController.activateWorkflowVersion("test-workflow", "1");
    verify(actionHandler, times(1)).activate(eq("test-workflow"), eq("1"), any());
    // actionController.pauseWorkflowVersion("test-workflow", "latest");
    // verify(actionHandler, times(1)).pause("test-workflow", "latest");
    // actionController.resumeWorkflowVersion("test-workflow", "latest");
    // verify(actionHandler, times(1)).resume("test-workflow", "latest");
  }

  @Test
  public void testUnblock() {
    actionController.unblockWorkflow("test-workflow");
    verify(actionHandler, times(1)).unblock(eq("test-workflow"), any());
  }
}
