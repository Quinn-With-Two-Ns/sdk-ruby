# frozen_string_literal: true

require 'nexus_rpc'
require 'temporalio/client'
require 'temporalio/error'
require 'temporalio/testing'
require 'temporalio/worker'
require 'temporalio/workflow'
require 'test'

class WorkerNexusHandlerTest < Test
  # ── Service definitions ────────────────────────────────────────────────

  class MyNexusService < NexusRPC::Service
    service_name 'my-nexus-service'

    operation :Echo, input: String, output: String
    operation :RaiseHandlerError, name: 'raise-handler-error', input: String, output: String
    operation :RaiseOperationError, name: 'raise-operation-error', input: String, output: String
    operation :RaiseApplicationError, name: 'raise-application-error', input: String, output: String
    operation :RunWorkflow, name: 'run-workflow', input: String, output: String
    operation :ContextCheck, name: 'context-check', input: String, output: Hash
    operation :TypedInput, name: 'typed-input', input: String, output: String
  end

  class SecondNexusService < NexusRPC::Service
    service_name 'second-nexus-service'

    operation :Greet, input: String, output: String
  end

  # ── Custom async operation handler ─────────────────────────────────────

  class CustomTokenAsyncHandler < NexusRPC::OperationHandler
    def start(_ctx, _input)
      NexusRPC::HandlerStartOperationResult::Async.new(token: 'custom-token-123')
    end

    def cancel(_ctx, _token)
      # no-op
    end
  end

  # ── Backing workflows for WorkflowRunOperation ─────────────────────────

  class NexusBackingSuccessWorkflow < Temporalio::Workflow::Definition
    def execute(input)
      "workflow-result-#{input}"
    end
  end

  class NexusBackingCancelWorkflow < Temporalio::Workflow::Definition
    def execute(_input)
      Temporalio::Workflow.wait_condition { false }
    end
  end

  # ── Service handlers ───────────────────────────────────────────────────

  class MyNexusServiceHandler < NexusRPC::ServiceHandler
    service MyNexusService

    sync_operation
    def echo(_ctx, input)
      "echo: #{input}"
    end

    sync_operation
    def raise_handler_error(_ctx, _input)
      raise NexusRPC::HandlerError.new('intentional handler error', type: NexusRPC::HandlerErrorType::BAD_REQUEST)
    end

    sync_operation
    def raise_operation_error(_ctx, _input)
      raise NexusRPC::OperationError.new('intentional operation error', state: NexusRPC::OperationState::FAILED)
    end

    sync_operation
    def raise_application_error(_ctx, _input)
      raise Temporalio::Error::ApplicationError.new('intentional app error', type: 'MyAppError', non_retryable: true)
    end

    sync_operation
    def context_check(_ctx, _input)
      info = Temporalio::Nexus.info
      {
        'in_operation' => Temporalio::Nexus.in_operation?,
        'has_client' => !Temporalio::Nexus.client.nil?,
        'task_queue' => info.task_queue,
        'service' => info.service,
        'operation' => info.operation,
        'worker_shutdown' => Temporalio::Nexus.worker_shutdown?
      }
    end

    sync_operation
    def typed_input(_ctx, input)
      "typed: #{input}"
    end

    operation_handler :run_workflow do
      Temporalio::Nexus::WorkflowRunOperationHandler.new do |_ctx, input|
        start_workflow(
          NexusBackingSuccessWorkflow,
          input,
          id: "nexus-backing-wf-#{SecureRandom.uuid}"
        )
      end
    end
  end

  class SecondNexusServiceHandler < NexusRPC::ServiceHandler
    service SecondNexusService

    sync_operation
    def greet(_ctx, input)
      "hello: #{input}"
    end
  end

  # Handler that uses workflow-run with cancel-friendly backing workflow
  class CancelableServiceHandler < NexusRPC::ServiceHandler
    service MyNexusService

    sync_operation
    def echo(_ctx, input)
      "echo: #{input}"
    end

    sync_operation
    def raise_handler_error(_ctx, _input)
      raise NexusRPC::HandlerError.new('intentional handler error', type: NexusRPC::HandlerErrorType::BAD_REQUEST)
    end

    sync_operation
    def raise_operation_error(_ctx, _input)
      raise NexusRPC::OperationError.new('intentional operation error', state: NexusRPC::OperationState::FAILED)
    end

    sync_operation
    def raise_application_error(_ctx, _input)
      raise Temporalio::Error::ApplicationError.new('intentional app error', type: 'MyAppError', non_retryable: true)
    end

    sync_operation
    def context_check(_ctx, _input)
      {}
    end

    sync_operation
    def typed_input(_ctx, input)
      "typed: #{input}"
    end

    operation_handler :run_workflow do
      Temporalio::Nexus::WorkflowRunOperationHandler.new do |_ctx, input|
        start_workflow(
          NexusBackingCancelWorkflow,
          input,
          id: "nexus-cancel-backing-wf-#{SecureRandom.uuid}"
        )
      end
    end
  end

  # ── Custom token service + handler ─────────────────────────────────────

  class CustomTokenService < NexusRPC::Service
    service_name 'custom-token-service'

    operation :AsyncOp, name: 'async-op', input: String, output: String
  end

  class CustomTokenServiceHandler < NexusRPC::ServiceHandler
    service CustomTokenService

    operation_handler :async_op, CustomTokenAsyncHandler
  end

  # ── Caller workflows ──────────────────────────────────────────────────

  class SyncOpCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('echo', 'hello')
    end
  end

  class SyncOpErrorCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('raise-handler-error', 'input')
    end
  end

  class WorkflowRunCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('run-workflow', 'test-input')
    end
  end

  class WorkflowRunCancelCallerWorkflow < Temporalio::Workflow::Definition
    workflow_query_attr_reader :started

    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')

      cancellation, cancel_proc = Temporalio::Cancellation.new
      handle = client.start_operation(
        'run-workflow',
        'cancel-input',
        cancellation_type: Temporalio::Workflow::NexusOperationCancellationType::TRY_CANCEL,
        cancellation:
      )

      @started = true

      Temporalio::Workflow.sleep(0.01)
      cancel_proc.call
      Temporalio::Workflow.sleep(0.01)

      begin
        handle.result
      rescue Temporalio::Error::NexusOperationError => e
        return { 'cancelled' => true } if e.cause.is_a?(Temporalio::Error::CanceledError)

        raise
      end

      { 'cancelled' => false }
    end
  end

  class OperationErrorCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('raise-operation-error', 'input')
    end
  end

  class ApplicationErrorCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('raise-application-error', 'input')
    end
  end

  class ContextCheckCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('context-check', 'input')
    end
  end

  class TypedInputCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: MyNexusService)
      client.execute_operation(MyNexusService::TypedInput, 'typed-value')
    end
  end

  class MultiServiceCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client1 = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client2 = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'second-nexus-service')

      result1 = client1.execute_operation('echo', 'from-first')
      result2 = client2.execute_operation('greet', 'from-second')

      { 'first' => result1, 'second' => result2 }
    end
  end

  class LinkCheckCallerWorkflow < Temporalio::Workflow::Definition
    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')
      client.execute_operation('run-workflow', 'link-test')
    end
  end

  class CustomTokenCallerWorkflow < Temporalio::Workflow::Definition
    workflow_query_attr_reader :operation_token

    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'custom-token-service')
      handle = client.start_operation('async-op', 'input')
      @operation_token = handle.operation_token
      # Don't wait for result since there's no backing workflow to complete it
      Temporalio::Workflow.wait_condition { false }
    end
  end

  class CancelSyncCallerWorkflow < Temporalio::Workflow::Definition
    workflow_query_attr_reader :started

    def execute(endpoint)
      client = Temporalio::Workflow.create_nexus_client(endpoint:, service: 'my-nexus-service')

      cancellation, cancel_proc = Temporalio::Cancellation.new
      handle = client.start_operation(
        'echo',
        'cancel-test',
        cancellation_type: Temporalio::Workflow::NexusOperationCancellationType::TRY_CANCEL,
        cancellation:
      )

      @started = true

      Temporalio::Workflow.sleep(0.01)
      cancel_proc.call
      Temporalio::Workflow.sleep(0.01)

      # Sync operations complete immediately so the result should already be available
      handle.result
    end
  end

  # ── Helper: run a single worker with both caller workflows and nexus handlers ──

  def run_nexus_handler_test(
    caller_workflow,
    *args,
    handler_instances: [MyNexusServiceHandler.new],
    handler_workflows: [],
    caller_workflows: []
  )
    task_queue = "tq-#{SecureRandom.uuid}"
    endpoint_name = "nexus-endpoint-#{task_queue}"
    endpoint = env.server.create_nexus_endpoint(name: endpoint_name, task_queue: task_queue)

    begin
      all_workflows = [caller_workflow] + caller_workflows + handler_workflows

      worker = Temporalio::Worker.new(
        client: env.client,
        task_queue: task_queue,
        activities: [],
        workflows: all_workflows,
        nexus_service_handlers: handler_instances,
        logger: env.client.options.logger
      )

      worker.run do
        handle = env.client.start_workflow(
          caller_workflow,
          *([endpoint_name] + args),
          id: "wf-#{SecureRandom.uuid}",
          task_queue: task_queue
        )
        if block_given?
          yield handle
        else
          handle.result
        end
      end
    ensure
      env.server.delete_nexus_endpoint(endpoint)
    end
  end

  # ── Tests ──────────────────────────────────────────────────────────────

  # 1. Sync operation success
  def test_sync_operation_success
    result = run_nexus_handler_test(SyncOpCallerWorkflow)
    assert_equal 'echo: hello', result
  end

  # 2. Sync operation handler error
  def test_sync_operation_handler_error
    err = assert_raises(Temporalio::Error::WorkflowFailedError) do
      run_nexus_handler_test(SyncOpErrorCallerWorkflow)
    end
    assert_instance_of Temporalio::Error::NexusOperationError, err.cause
    assert_includes err.cause.message, 'nexus operation completed unsuccessfully'
  end

  # 3. WorkflowRunOperation success
  def test_workflow_run_operation_success
    result = run_nexus_handler_test(
      WorkflowRunCallerWorkflow,
      handler_workflows: [NexusBackingSuccessWorkflow]
    )
    assert_equal 'workflow-result-test-input', result
  end

  # 4. WorkflowRunOperation cancel
  def test_workflow_run_operation_cancel
    result = run_nexus_handler_test(
      WorkflowRunCancelCallerWorkflow,
      handler_instances: [CancelableServiceHandler.new],
      handler_workflows: [NexusBackingCancelWorkflow]
    ) do |handle|
      assert_eventually { assert handle.query(WorkflowRunCancelCallerWorkflow.started) }
      handle.result
    end
    assert result['cancelled'] # steep:ignore
  end

  # 5. OperationError handling (distinct from handler error)
  def test_operation_error
    err = assert_raises(Temporalio::Error::WorkflowFailedError) do
      run_nexus_handler_test(OperationErrorCallerWorkflow)
    end
    assert_instance_of Temporalio::Error::NexusOperationError, err.cause
    assert_includes err.cause.message, 'nexus operation completed unsuccessfully'
  end

  # 6. Handler error mapping - ApplicationError maps to internal handler error
  def test_application_error_mapping
    err = assert_raises(Temporalio::Error::WorkflowFailedError) do
      run_nexus_handler_test(ApplicationErrorCallerWorkflow)
    end
    assert_instance_of Temporalio::Error::NexusOperationError, err.cause
    assert_includes err.cause.message, 'nexus operation completed unsuccessfully'
  end

  # 7. Context accessors
  def test_context_accessors
    result = run_nexus_handler_test(ContextCheckCallerWorkflow)
    assert_equal true, result['in_operation']
    assert_equal true, result['has_client']
    assert_equal 'my-nexus-service', result['service']
    assert_equal 'context-check', result['operation']
    refute_nil result['task_queue']
    assert_equal false, result['worker_shutdown']
  end

  # 8. Link propagation - verify links exist in history for workflow-run operations
  def test_link_propagation
    run_nexus_handler_test(
      LinkCheckCallerWorkflow,
      handler_workflows: [NexusBackingSuccessWorkflow]
    ) do |handle|
      result = handle.result
      assert_equal 'workflow-result-link-test', result

      # Verify the caller workflow history has a NexusOperationScheduled event
      history_events = handle.fetch_history_events.to_a
      scheduled_event = history_events.find(&:nexus_operation_scheduled_event_attributes)
      refute_nil scheduled_event, 'Expected NexusOperationScheduled event in history'

      # Verify there is a NexusOperationCompleted event
      completed_event = history_events.find(&:nexus_operation_completed_event_attributes)
      refute_nil completed_event, 'Expected NexusOperationCompleted event in history'
    end
  end

  # 9. Type hint deserialization
  def test_type_hint_deserialization
    result = run_nexus_handler_test(TypedInputCallerWorkflow)
    assert_equal 'typed: typed-value', result
  end

  # 10. Multiple services on same worker
  def test_multiple_services_on_same_worker
    result = run_nexus_handler_test(
      MultiServiceCallerWorkflow,
      handler_instances: [MyNexusServiceHandler.new, SecondNexusServiceHandler.new]
    )
    assert_equal 'echo: from-first', result['first']
    assert_equal 'hello: from-second', result['second']
  end

  # 11. Async operation with custom token
  def test_async_operation_custom_token
    task_queue = "tq-#{SecureRandom.uuid}"
    endpoint_name = "nexus-endpoint-#{task_queue}"
    endpoint = env.server.create_nexus_endpoint(name: endpoint_name, task_queue: task_queue)

    begin
      worker = Temporalio::Worker.new(
        client: env.client,
        task_queue: task_queue,
        activities: [],
        workflows: [CustomTokenCallerWorkflow],
        nexus_service_handlers: [CustomTokenServiceHandler.new],
        logger: env.client.options.logger
      )

      worker.run do
        handle = env.client.start_workflow(
          CustomTokenCallerWorkflow,
          endpoint_name,
          id: "wf-#{SecureRandom.uuid}",
          task_queue: task_queue
        )

        assert_eventually do
          token = handle.query(CustomTokenCallerWorkflow.operation_token)
          refute_nil token
          assert_equal 'custom-token-123', token
        end

        handle.cancel
      end
    ensure
      env.server.delete_nexus_endpoint(endpoint)
    end
  end

  # 12. Sync operation cancel completes normally (sync ops are immediate)
  def test_sync_operation_cancel_completes_normally
    result = run_nexus_handler_test(CancelSyncCallerWorkflow) do |handle|
      assert_eventually { assert handle.query(CancelSyncCallerWorkflow.started) }
      handle.result
    end
    assert_equal 'echo: cancel-test', result
  end

  # 13. Nexus inbound interceptor
  class TestNexusInterceptor
    include Temporalio::Worker::Interceptor::Nexus

    attr_reader :start_calls, :cancel_calls

    def initialize
      @start_calls = []
      @cancel_calls = []
    end

    def intercept_nexus_operation(next_interceptor)
      TestNexusInbound.new(next_interceptor, self)
    end

    class TestNexusInbound < Temporalio::Worker::Interceptor::Nexus::Inbound
      def initialize(next_interceptor, interceptor)
        super(next_interceptor)
        @interceptor = interceptor
      end

      def execute_operation_start(input)
        @interceptor.start_calls << input.ctx.operation
        @next_interceptor.execute_operation_start(input)
      end

      def execute_operation_cancel(input)
        @interceptor.cancel_calls << input.ctx.operation
        @next_interceptor.execute_operation_cancel(input)
      end
    end
  end

  def test_nexus_interceptor
    interceptor = TestNexusInterceptor.new
    task_queue = "tq-#{SecureRandom.uuid}"
    endpoint_name = "nexus-endpoint-#{task_queue}"
    endpoint = env.server.create_nexus_endpoint(name: endpoint_name, task_queue: task_queue)

    begin
      worker = Temporalio::Worker.new(
        client: env.client,
        task_queue: task_queue,
        activities: [],
        workflows: [SyncOpCallerWorkflow],
        nexus_service_handlers: [MyNexusServiceHandler.new],
        interceptors: [interceptor],
        logger: env.client.options.logger
      )

      worker.run do
        handle = env.client.start_workflow(
          SyncOpCallerWorkflow,
          endpoint_name,
          id: "wf-#{SecureRandom.uuid}",
          task_queue: task_queue
        )
        result = handle.result
        assert_equal 'echo: hello', result
      end

      assert_includes interceptor.start_calls, 'echo'
    ensure
      env.server.delete_nexus_endpoint(endpoint)
    end
  end
end
