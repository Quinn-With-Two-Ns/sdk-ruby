# frozen_string_literal: true

require 'json'
require 'temporalio/internal/bridge/api'
require 'temporalio/nexus'
require 'temporalio/internal/worker/nexus_operation_context'
require 'temporalio/nexus/link_conversion'
require 'temporalio/worker/interceptor'

module Temporalio
  module Internal
    module Worker
      # Worker for handling Nexus tasks. Follows the same pattern as ActivityWorker.
      #
      # @!visibility private
      class NexusWorker
        TEMPORAL_FAILURE_PROTO_TYPE = 'temporal.api.failure.v1.Failure'

        attr_reader :worker, :bridge_worker

        def initialize(worker:, bridge_worker:)
          @worker = worker
          @bridge_worker = bridge_worker
          @runtime_metric_meter = worker.options.client.connection.options.runtime.metric_meter
          @logger = worker.options.logger
          @data_converter = worker.options.client.data_converter
          @worker_shutdown_cancellation = worker._worker_shutdown_cancellation
          @interceptors = worker.options.interceptors

          # Build service handler lookup: service_name -> handler instance
          @service_handlers = {}
          worker.options.nexus_service_handlers.each do |handler|
            handler.class.send(:validate!)
            service_defn = handler.class.service_definition
            raise ArgumentError, "No service definition on #{handler.class}" unless service_defn

            service_name = service_defn.service_name
            if @service_handlers.key?(service_name)
              raise ArgumentError, "Duplicate Nexus service handler for service '#{service_name}'"
            end

            @service_handlers[service_name] = handler
          end

          # Track running tasks
          @running_tasks_mutex = Mutex.new
          @running_tasks = {}
          @running_tasks_empty_condvar = ConditionVariable.new
        end

        def handle_task(bytes)
          nexus_task = Bridge::Api::Nexus::NexusTask.decode(bytes)

          if nexus_task.variant == :task
            task = nexus_task.task
            task_token = task.task_token
            request = task.request
            request_deadline = _proto_timestamp_to_time(nexus_task.request_deadline)

            # Create a task cancellation token for this task
            task_cancellation = NexusRPC::OperationTaskCancellation.new
            set_running_task(task_token, task_cancellation)

            if request.variant == :start_operation
              handle_start_operation(task_token, request.start_operation, request.header.to_h,
                                    request_deadline, task_cancellation)
            elsif request.variant == :cancel_operation
              handle_cancel_operation(task_token, request.cancel_operation, request.header.to_h,
                                     request_deadline, task_cancellation)
            else
              raise "Unrecognized Nexus task request variant: #{request.variant}"
            end
          elsif nexus_task.variant == :cancel_task
            cancel_task = nexus_task.cancel_task
            handle_cancel_task(cancel_task.task_token, cancel_task.reason)
          else
            raise "Unrecognized Nexus task variant: #{nexus_task.variant}"
          end
        end

        def wait_all_complete
          @running_tasks_mutex.synchronize do
            @running_tasks_empty_condvar.wait(@running_tasks_mutex) until @running_tasks.empty?
          end
        end

        private

        def set_running_task(task_token, task_cancellation)
          @running_tasks_mutex.synchronize do
            @running_tasks[task_token] = task_cancellation
          end
        end

        def remove_running_task(task_token)
          @running_tasks_mutex.synchronize do
            @running_tasks.delete(task_token)
            @running_tasks_empty_condvar.broadcast if @running_tasks.empty?
          end
        end

        def handle_cancel_task(task_token, reason)
          task_cancellation = @running_tasks_mutex.synchronize { @running_tasks[task_token] }
          if task_cancellation
            reason_str = reason == :TIMED_OUT ? 'timed_out' : 'worker_shutdown'
            task_cancellation.cancel(reason: reason_str)
          else
            @logger.debug("Received cancel_task for unknown token: #{task_token}")
          end
        end

        def handle_start_operation(task_token, start_request, headers, request_deadline, task_cancellation)
          # Look up the service handler
          handler = @service_handlers[start_request.service]
          unless handler
            send_handler_error_completion(
              task_token,
              NexusRPC::HandlerError.new(
                "No handler for service '#{start_request.service}'",
                type: NexusRPC::HandlerErrorType::NOT_FOUND
              )
            )
            remove_running_task(task_token)
            return
          end

          # Get the operation definition for type hints (lookup by wire name)
          service_defn = handler.class.service_definition
          operation_defn = service_defn.operations.values.find { |op| op.name == start_request.operation }

          # Build the NexusRPC StartOperationContext
          inbound_links = start_request.links.map do |link|
            NexusRPC::Link.new(url: link.url, type: link.type)
          end
          outbound_links = []

          ctx = NexusRPC::StartOperationContext.new(
            service: start_request.service,
            operation: start_request.operation,
            headers: headers,
            callback_url: start_request.callback,
            request_id: start_request.request_id,
            callback_headers: start_request.callback_header.to_h,
            inbound_links: inbound_links,
            outbound_links: outbound_links,
            request_deadline: request_deadline,
            task_cancellation: task_cancellation
          )

          # Set the Temporal operation context
          temporal_context = NexusOperationContext.new(
            client: @worker.options.client,
            task_queue: @worker.options.task_queue,
            data_converter: @data_converter,
            metric_meter: @runtime_metric_meter,
            inbound_links: inbound_links,
            outbound_links: outbound_links,
            callback_url: start_request.callback,
            callback_headers: start_request.callback_header.to_h,
            request_id: start_request.request_id,
            info: Temporalio::Nexus::Info.new(
              task_queue: @worker.options.task_queue,
              service: start_request.service,
              operation: start_request.operation
            ),
            worker_shutdown_cancellation: @worker_shutdown_cancellation
          )
          NexusOperationContext._current_raw = temporal_context

          begin
            # Deserialize input
            input = _deserialize_input(start_request.payload, operation_defn)

            # Build interceptor chain and dispatch
            impl = StartInboundImplementation.new(handler, start_request.operation)
            inbound = @interceptors.select { |i| i.is_a?(Temporalio::Worker::Interceptor::Nexus) }
                                   .reverse_each.reduce(impl) { |acc, int| int.intercept_nexus_operation(acc) }
            result = inbound.execute_operation_start(
              Temporalio::Worker::Interceptor::Nexus::ExecuteOperationStartInput.new(
                ctx: ctx,
                input: input
              )
            )

            # Build response based on result type
            links = outbound_links.map do |link|
              Api::Nexus::V1::Link.new(url: link.url.to_s, type: link.type.to_s)
            end

            start_response = if result.is_a?(NexusRPC::HandlerStartOperationResult::Async)
                               Api::Nexus::V1::StartOperationResponse.new(
                                 async_success: Api::Nexus::V1::StartOperationResponse::Async.new(
                                   operation_token: result.token,
                                   links: links
                                 )
                               )
                             elsif result.is_a?(NexusRPC::HandlerStartOperationResult::Sync)
                               payload = @data_converter.to_payload(result.value)
                               Api::Nexus::V1::StartOperationResponse.new(
                                 sync_success: Api::Nexus::V1::StartOperationResponse::Sync.new(
                                   payload: payload,
                                   links: links
                                 )
                               )
                             else
                               raise TypeError,
                                     'Operation start method must return either ' \
                                     'NexusRPC::HandlerStartOperationResult::Sync or ' \
                                     'NexusRPC::HandlerStartOperationResult::Async'
                             end

            completion = Bridge::Api::Nexus::NexusTaskCompletion.new(
              task_token: task_token,
              completed: Api::Nexus::V1::Response.new(
                start_operation: start_response
              )
            )
            @bridge_worker.complete_nexus_task(completion)
          rescue NexusRPC::OperationError => e
            # OperationError is a DISTINCT path - returns operation_error in StartOperationResponse
            start_response = Api::Nexus::V1::StartOperationResponse.new(
              operation_error: _operation_error_to_proto(e)
            )
            completion = Bridge::Api::Nexus::NexusTaskCompletion.new(
              task_token: task_token,
              completed: Api::Nexus::V1::Response.new(
                start_operation: start_response
              )
            )
            @bridge_worker.complete_nexus_task(completion)
          rescue Exception => e # rubocop:disable Lint/RescueException
            @logger.warn("Failed to execute Nexus start operation method for #{start_request.operation}")
            @logger.warn(e)
            handler_error = _exception_to_handler_error(e)
            send_handler_error_completion(task_token, handler_error)
          ensure
            NexusOperationContext._current_raw = nil
            remove_running_task(task_token)
          end
        end

        def handle_cancel_operation(task_token, cancel_request, headers, request_deadline, task_cancellation)
          # Look up the service handler
          handler = @service_handlers[cancel_request.service]
          unless handler
            send_handler_error_completion(
              task_token,
              NexusRPC::HandlerError.new(
                "No handler for service '#{cancel_request.service}'",
                type: NexusRPC::HandlerErrorType::NOT_FOUND
              )
            )
            remove_running_task(task_token)
            return
          end

          # Build the NexusRPC CancelOperationContext
          ctx = NexusRPC::CancelOperationContext.new(
            service: cancel_request.service,
            operation: cancel_request.operation,
            token: cancel_request.operation_token,
            headers: headers,
            request_deadline: request_deadline,
            task_cancellation: task_cancellation
          )

          # Set the Temporal operation context for cancel
          temporal_context = NexusOperationContext.new(
            client: @worker.options.client,
            task_queue: @worker.options.task_queue,
            data_converter: @data_converter,
            metric_meter: @runtime_metric_meter,
            inbound_links: [],
            outbound_links: [],
            callback_url: nil,
            callback_headers: {},
            request_id: nil,
            info: Temporalio::Nexus::Info.new(
              task_queue: @worker.options.task_queue,
              service: cancel_request.service,
              operation: cancel_request.operation
            ),
            worker_shutdown_cancellation: @worker_shutdown_cancellation
          )
          NexusOperationContext._current_raw = temporal_context

          begin
            # Build interceptor chain and dispatch
            impl = CancelInboundImplementation.new(handler, cancel_request.operation)
            inbound = @interceptors.select { |i| i.is_a?(Temporalio::Worker::Interceptor::Nexus) }
                                   .reverse_each.reduce(impl) { |acc, int| int.intercept_nexus_operation(acc) }
            inbound.execute_operation_cancel(
              Temporalio::Worker::Interceptor::Nexus::ExecuteOperationCancelInput.new(
                ctx: ctx,
                token: cancel_request.operation_token
              )
            )

            # Successful cancel completion
            completion = Bridge::Api::Nexus::NexusTaskCompletion.new(
              task_token: task_token,
              completed: Api::Nexus::V1::Response.new(
                cancel_operation: Api::Nexus::V1::CancelOperationResponse.new
              )
            )
            @bridge_worker.complete_nexus_task(completion)
          rescue Exception => e # rubocop:disable Lint/RescueException
            @logger.warn("Failed to execute Nexus cancel operation method for #{cancel_request.operation}")
            @logger.warn(e)
            handler_error = _exception_to_handler_error(e)
            send_handler_error_completion(task_token, handler_error)
          ensure
            NexusOperationContext._current_raw = nil
            remove_running_task(task_token)
          end
        end

        def _proto_timestamp_to_time(timestamp)
          return nil if timestamp.nil? || (timestamp.seconds.zero? && timestamp.nanos.zero?)

          Time.at(timestamp.seconds, timestamp.nanos, :nanosecond).utc
        end

        def _deserialize_input(payload, operation_defn)
          return nil if payload.nil? || (payload.data.nil? && payload.metadata.empty?)

          type_hint = operation_defn&.input_type
          @data_converter.from_payload(payload, hint: type_hint)
        end

        def _exception_to_handler_error(err)
          if err.is_a?(NexusRPC::HandlerError)
            return err
          elsif err.is_a?(Error::ApplicationError)
            handler_err = NexusRPC::HandlerError.new(
              err.message,
              type: NexusRPC::HandlerErrorType::INTERNAL,
              retryable: !err.non_retryable
            )
          elsif err.is_a?(Error::WorkflowAlreadyStartedError)
            handler_err = NexusRPC::HandlerError.new(
              err.message,
              type: NexusRPC::HandlerErrorType::INTERNAL,
              retryable: false
            )
          elsif err.is_a?(Error::RPCError)
            handler_err = _rpc_error_to_handler_error(err)
          else
            handler_err = NexusRPC::HandlerError.new(
              err.message,
              type: NexusRPC::HandlerErrorType::INTERNAL
            )
          end
          Error._with_backtrace_and_cause(handler_err, backtrace: err.backtrace, cause: err)
        end

        def _rpc_error_to_handler_error(err) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/MethodLength
          # Mapping based on Python SDK _exception_to_handler_error
          case err.code
          when Error::RPCError::Code::INVALID_ARGUMENT
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::BAD_REQUEST)
          when Error::RPCError::Code::ALREADY_EXISTS,
               Error::RPCError::Code::FAILED_PRECONDITION,
               Error::RPCError::Code::OUT_OF_RANGE
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::INTERNAL, retryable: false)
          when Error::RPCError::Code::ABORTED,
               Error::RPCError::Code::UNAVAILABLE
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::UNAVAILABLE)
          when Error::RPCError::Code::CANCELLED,
               Error::RPCError::Code::DATA_LOSS,
               Error::RPCError::Code::INTERNAL,
               Error::RPCError::Code::UNKNOWN,
               Error::RPCError::Code::UNAUTHENTICATED,
               Error::RPCError::Code::PERMISSION_DENIED
            # Note: UNAUTHENTICATED and PERMISSION_DENIED map to INTERNAL because this is not a client
            # auth error -- it happens when the handler fails to auth with Temporal and should be retryable.
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::INTERNAL)
          when Error::RPCError::Code::NOT_FOUND
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::NOT_FOUND)
          when Error::RPCError::Code::RESOURCE_EXHAUSTED
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::RESOURCE_EXHAUSTED)
          when Error::RPCError::Code::UNIMPLEMENTED
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::NOT_IMPLEMENTED)
          when Error::RPCError::Code::DEADLINE_EXCEEDED
            NexusRPC::HandlerError.new(err.message, type: NexusRPC::HandlerErrorType::UPSTREAM_TIMEOUT)
          else
            NexusRPC::HandlerError.new(
              "Unhandled RPC error status: #{err.code}",
              type: NexusRPC::HandlerErrorType::INTERNAL
            )
          end
        end

        def _operation_error_to_proto(err)
          Api::Nexus::V1::UnsuccessfulOperationError.new(
            operation_state: err.state.to_s,
            failure: _nexus_error_to_failure_proto(err)
          )
        end

        def _nexus_error_to_failure_proto(error)
          cause = error.cause
          if cause
            begin
              failure = @data_converter.to_failure(cause)
              # Get the message from the failure proto, then clear it so it doesn't appear in details
              message = failure.message.empty? ? error.message : failure.message
              failure.message = ''
              # Encode the remaining failure as JSON for the details field
              # Following other SDKs, the message from the cause chain is moved to
              # the top-level nexus.v1.Failure message.
              json_str = failure.class.encode_json(failure, emit_defaults: false)
              # Parse and remove empty/default values
              json_hash = JSON.parse(json_str)
              json_hash.delete('message') # Already moved to top-level
              json_hash.delete_if { |_, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
              details = json_hash.empty? ? nil : JSON.generate(json_hash)
              Api::Nexus::V1::Failure.new(
                message: message,
                metadata: { 'type' => TEMPORAL_FAILURE_PROTO_TYPE },
                details: details&.encode('UTF-8')
              )
            rescue StandardError => e
              @logger.warn("Failed to serialize cause chain of nexus exception: #{e.message}")
              Api::Nexus::V1::Failure.new(message: error.message)
            end
          else
            Api::Nexus::V1::Failure.new(message: error.message)
          end
        end

        # @!visibility private
        class StartInboundImplementation < Temporalio::Worker::Interceptor::Nexus::Inbound
          def initialize(handler, operation_name) # rubocop:disable Lint/MissingSuper
            @handler = handler
            @operation_name = operation_name
          end

          def execute_operation_start(input)
            @handler.dispatch_start(@operation_name, input.ctx, input.input)
          end
        end

        # @!visibility private
        class CancelInboundImplementation < Temporalio::Worker::Interceptor::Nexus::Inbound
          def initialize(handler, operation_name) # rubocop:disable Lint/MissingSuper
            @handler = handler
            @operation_name = operation_name
          end

          def execute_operation_cancel(input)
            @handler.dispatch_cancel(@operation_name, input.ctx, input.token)
          end
        end

        def send_handler_error_completion(task_token, handler_error)
          # Use retryable_override (not retryable) to preserve three-state semantics
          # (nil = unspecified, true = retryable, false = non-retryable) for the proto mapping.
          retry_behavior = if handler_error.retryable_override.nil?
                             :NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
                           elsif handler_error.retryable_override
                             :NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
                           else
                             :NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
                           end

          completion = Bridge::Api::Nexus::NexusTaskCompletion.new(
            task_token: task_token,
            error: Api::Nexus::V1::HandlerError.new(
              error_type: handler_error.type.to_s.upcase,
              failure: _nexus_error_to_failure_proto(handler_error),
              retry_behavior: retry_behavior
            )
          )
          @bridge_worker.complete_nexus_task(completion)
        rescue StandardError => e
          @logger.error("Failed to send Nexus task error completion: #{e.message}")
          @logger.error(e)
        end
      end
    end
  end
end
