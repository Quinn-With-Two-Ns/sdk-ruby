# frozen_string_literal: true

require 'nexus_rpc/operation_handler'
require 'temporalio/internal/worker/nexus_operation_context'
require 'temporalio/nexus/link_conversion'
require 'temporalio/nexus/workflow_handle'

module Temporalio
  module Nexus
    # Operation handler for Nexus operations backed by a workflow.
    #
    # Use this class to create an operation handler that starts a workflow by passing a block to the constructor.
    # The block should call {#start_workflow} to start the workflow and return the resulting {WorkflowHandle}.
    #
    # WARNING: Nexus support is experimental.
    class WorkflowRunOperationHandler < NexusRPC::OperationHandler
      # Create a new workflow run operation handler.
      #
      # @yield [ctx, input] Block called when the operation is started. Must call {#start_workflow} and return the
      #   resulting {WorkflowHandle}.
      def initialize(&start_block)
        super()
        raise ArgumentError, 'WorkflowRunOperationHandler requires a block' unless start_block

        @start_block = start_block
      end

      # Start the operation by invoking the user's block, which should call {#start_workflow}.
      #
      # @param ctx [NexusRPC::StartOperationContext] The start operation context.
      # @param input [Object] Deserialized input for the operation.
      # @return [NexusRPC::HandlerStartOperationResult::Async] Async result with the operation token.
      def start(ctx, input)
        handle = instance_exec(ctx, input, &@start_block)
        unless handle.is_a?(Temporalio::Nexus::WorkflowHandle)
          raise 'Expected start block to return a Temporalio::Nexus::WorkflowHandle. ' \
                'Use WorkflowRunOperationHandler#start_workflow to start a workflow-backed Nexus operation.'
        end

        NexusRPC::HandlerStartOperationResult::Async.new(token: handle.to_token)
      end

      # Cancel the operation by canceling the backing workflow.
      #
      # @param _ctx [NexusRPC::CancelOperationContext] The cancel operation context.
      # @param token [String] The operation token.
      def cancel(_ctx, token)
        begin
          nexus_handle = Temporalio::Nexus::WorkflowHandle.from_token(token)
        rescue StandardError => e
          raise NexusRPC::HandlerError.new(
            "Failed to decode operation token as a workflow operation token. " \
            "Canceling non-workflow operations is not supported. Cause: #{e.message}",
            type: NexusRPC::HandlerErrorType::NOT_FOUND
          )
        end

        ctx = Temporalio::Internal::Worker::NexusOperationContext.current
        client_handle = ctx.client.workflow_handle(nexus_handle.workflow_id)
        client_handle.cancel
      end

      # Start a workflow that will deliver the result of this Nexus operation.
      #
      # The workflow will be started in the same namespace as the Nexus worker, using the same client as the worker.
      # If task queue is not specified, the worker's task queue will be used.
      #
      # On workflow completion, Temporal server will deliver the workflow result to the Nexus operation caller via the
      # callback from the Nexus operation start request. The request ID from the Nexus start request will be used as the
      # request ID for the StartWorkflow request. Inbound links from the Nexus start request will be attached to the
      # started workflow, and outbound links will be added to the Nexus start operation response.
      #
      # @param workflow [Class<Workflow::Definition>, Symbol, String] Workflow definition class or workflow name.
      # @param args [Array<Object>] Arguments to the workflow.
      # @param id [String] Unique identifier for the workflow execution.
      # @param task_queue [String, nil] Task queue to run the workflow on. Defaults to the worker's task queue.
      # @param static_summary [String, nil] Fixed single-line summary for this workflow execution.
      # @param static_details [String, nil] Fixed details for this workflow execution.
      # @param execution_timeout [Float, nil] Total workflow execution timeout in seconds.
      # @param run_timeout [Float, nil] Timeout of a single workflow run in seconds.
      # @param task_timeout [Float, nil] Timeout of a single workflow task in seconds.
      # @param id_reuse_policy [WorkflowIDReusePolicy] How already-existing IDs are treated.
      # @param id_conflict_policy [WorkflowIDConflictPolicy] How already-running workflows of the same ID are treated.
      # @param retry_policy [RetryPolicy, nil] Retry policy for the workflow.
      # @param cron_schedule [String, nil] Cron schedule.
      # @param memo [Hash{String, Symbol => Object}, nil] Memo for the workflow.
      # @param search_attributes [SearchAttributes, nil] Search attributes for the workflow.
      # @param start_delay [Float, nil] Amount of time in seconds to wait before starting the workflow.
      # @param request_eager_start [Boolean] Potentially reduce the latency to start this workflow.
      # @param versioning_override [VersioningOverride, nil] Override the version of the workflow.
      # @param priority [Priority] Priority for the workflow.
      # @param arg_hints [Array<Object>, nil] Overrides converter hints for arguments.
      # @param result_hint [Object, nil] Overrides converter hint for result.
      # @param rpc_options [Client::RPCOptions, nil] Advanced RPC options.
      # @return [WorkflowHandle] Handle to the started workflow, suitable for returning from the start block.
      def start_workflow( # rubocop:disable Metrics/ParameterLists
        workflow,
        *args,
        id:,
        task_queue: nil,
        static_summary: nil,
        static_details: nil,
        execution_timeout: nil,
        run_timeout: nil,
        task_timeout: nil,
        id_reuse_policy: WorkflowIDReusePolicy::ALLOW_DUPLICATE,
        id_conflict_policy: WorkflowIDConflictPolicy::UNSPECIFIED,
        retry_policy: nil,
        cron_schedule: nil,
        memo: nil,
        search_attributes: nil,
        start_delay: nil,
        request_eager_start: false,
        versioning_override: nil,
        priority: Priority.default,
        arg_hints: nil,
        result_hint: nil,
        rpc_options: nil
      )
        temporal_context = Temporalio::Internal::Worker::NexusOperationContext.current
        client = temporal_context.client

        # Take hints from definition if there is a definition
        workflow, defn_arg_hints, defn_result_hint =
          Workflow::Definition._workflow_type_and_hints_from_workflow_parameter(workflow)

        # Use the task queue from context if not specified
        task_queue ||= temporal_context.task_queue

        # Build StartWorkflowInput (nexus-specific fields like callbacks, links, and
        # request_id are handled internally by the client implementation)
        input = Client::Interceptor::StartWorkflowInput.new(
          workflow: workflow,
          args: args,
          workflow_id: id,
          task_queue: task_queue,
          static_summary: static_summary,
          static_details: static_details,
          execution_timeout: execution_timeout,
          run_timeout: run_timeout,
          task_timeout: task_timeout,
          id_reuse_policy: id_reuse_policy,
          id_conflict_policy: id_conflict_policy,
          retry_policy: retry_policy,
          cron_schedule: cron_schedule,
          memo: memo,
          search_attributes: search_attributes,
          start_delay: start_delay,
          request_eager_start: request_eager_start,
          headers: {},
          versioning_override: versioning_override,
          priority: priority,
          arg_hints: arg_hints || defn_arg_hints,
          result_hint: result_hint || defn_result_hint,
          rpc_options: rpc_options
        )

        # Call through the client's internal implementation
        client_handle = client._impl.start_workflow(input)

        # Add outbound links
        _add_outbound_links(temporal_context, client_handle)

        # Return a Nexus WorkflowHandle (not a Client::WorkflowHandle)
        Temporalio::Nexus::WorkflowHandle.new(
          namespace: client.namespace,
          workflow_id: client_handle.id
        )
      end

      private

      def _add_outbound_links(temporal_context, client_handle)
        # Build a WorkflowEvent link for the started workflow
        return unless client_handle.first_execution_run_id

        workflow_event = Api::Common::V1::Link::WorkflowEvent.new(
          namespace: temporal_context.client.namespace,
          workflow_id: client_handle.id,
          run_id: client_handle.first_execution_run_id,
          request_id_ref: Api::Common::V1::Link::WorkflowEvent::RequestIdReference.new(
            request_id: temporal_context.request_id,
            event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
          )
        )

        nexus_link_url = LinkConversion.workflow_event_to_nexus_link(workflow_event)
        temporal_context.outbound_links << NexusRPC::Link.new(
          url: nexus_link_url,
          type: 'temporal.api.common.v1.Link.WorkflowEvent'
        )
      rescue StandardError => e
        warn "Failed to create WorkflowExecutionStarted event links for workflow #{client_handle.id}: #{e.message}"
      end
    end
  end
end
