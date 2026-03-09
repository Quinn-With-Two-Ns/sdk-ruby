# frozen_string_literal: true

require 'json'
require 'nexus_rpc'
require 'temporalio/api'
require 'temporalio/error'
require 'temporalio/internal/proto_utils'

module Temporalio
  module Converters
    # Base class for converting Ruby errors to/from Temporal failures.
    class FailureConverter
      TEMPORAL_FAILURE_PROTO_TYPE = 'temporal.api.failure.v1.Failure'
      # @return [FailureConverter] Default failure converter.
      def self.default
        @default ||= FailureConverter.new
      end

      # @return [Boolean] If +true+, the message and stack trace of the failure will be moved into the encoded attribute
      #   section of the failure which can be encoded with a codec.
      attr_reader :encode_common_attributes

      # Create failure converter.
      #
      # @param encode_common_attributes [Boolean] If +true+, the message and stack trace of the failure will be moved
      #   into the encoded attribute section of the failure which can be encoded with a codec.
      def initialize(encode_common_attributes: false)
        @encode_common_attributes = encode_common_attributes
      end

      # Convert a Ruby error to a Temporal failure.
      #
      # @param error [Exception] Ruby error.
      # @param converter [DataConverter, PayloadConverter] Converter for payloads.
      # @return [Api::Failure::V1::Failure] Converted failure.
      def to_failure(error, converter)
        failure = Api::Failure::V1::Failure.new(
          message: error.message,
          stack_trace: error.backtrace&.join("\n")
        )
        cause = error.cause
        failure.cause = to_failure(cause, converter) if cause

        # Convert specific error type details
        case error
        when Error::ApplicationError
          failure.application_failure_info = Api::Failure::V1::ApplicationFailureInfo.new(
            type: error.type,
            non_retryable: error.non_retryable,
            details: converter.to_payloads(error.details),
            next_retry_delay: Internal::ProtoUtils.seconds_to_duration(error.next_retry_delay),
            category: error.category
          )
        when Error::TimeoutError
          failure.timeout_failure_info = Api::Failure::V1::TimeoutFailureInfo.new(
            timeout_type: error.type,
            last_heartbeat_details: converter.to_payloads(error.last_heartbeat_details)
          )
        when Error::CanceledError
          failure.canceled_failure_info = Api::Failure::V1::CanceledFailureInfo.new(
            details: converter.to_payloads(error.details)
          )
        when Error::TerminatedError
          failure.terminated_failure_info = Api::Failure::V1::TerminatedFailureInfo.new
        when Error::ServerError
          failure.server_failure_info = Api::Failure::V1::ServerFailureInfo.new(
            non_retryable: error.non_retryable
          )
        when Error::ActivityError
          failure.activity_failure_info = Api::Failure::V1::ActivityFailureInfo.new(
            scheduled_event_id: error.scheduled_event_id,
            started_event_id: error.started_event_id,
            identity: error.identity,
            activity_type: Api::Common::V1::ActivityType.new(name: error.activity_type),
            activity_id: error.activity_id,
            retry_state: error.retry_state
          )
        when Error::ChildWorkflowError
          failure.child_workflow_execution_failure_info = Api::Failure::V1::ChildWorkflowExecutionFailureInfo.new(
            namespace: error.namespace,
            workflow_execution: Api::Common::V1::WorkflowExecution.new(
              workflow_id: error.workflow_id,
              run_id: error.run_id
            ),
            workflow_type: Api::Common::V1::WorkflowType.new(name: error.workflow_type),
            initiated_event_id: error.initiated_event_id,
            started_event_id: error.started_event_id,
            retry_state: error.retry_state
          )
        when Error::NexusOperationError
          failure.nexus_operation_execution_failure_info = Api::Failure::V1::NexusOperationFailureInfo.new(
            scheduled_event_id: error.scheduled_event_id,
            endpoint: error.endpoint,
            service: error.service,
            operation: error.operation,
            operation_token: error.operation_token || ''
          )
        when NexusRPC::HandlerError
          if error.failure
            # Round-trip: reconstitute the full temporal failure from the stored NexusRPC::Failure
            failure = _nexus_failure_to_temporal_failure(error.failure, error.retryable)
          else
            failure.nexus_handler_failure_info = Api::Failure::V1::NexusHandlerFailureInfo.new(
              type: error.type.to_s.upcase,
              retry_behavior: _retryable_override_to_retry_behavior(error.retryable_override)
            )
          end
        else
          failure.application_failure_info = Api::Failure::V1::ApplicationFailureInfo.new(
            type: error.class.name.to_s.split('::').last
          )
        end

        # If encoding common attributes, move message and stack trace
        if @encode_common_attributes
          failure.encoded_attributes = converter.to_payload(
            { message: failure.message, stack_trace: failure.stack_trace }
          )
          failure.message = 'Encoded failure'
          failure.stack_trace = ''
        end

        failure
      end

      # Convert a Temporal failure to a Ruby error.
      #
      # @param failure [Api::Failure::V1::Failure] Failure.
      # @param converter [DataConverter, PayloadConverter] Converter for payloads.
      # @return [Error::Failure] Converted Ruby error.
      def from_failure(failure, converter)
        # If encoded attributes have any of the fields we expect, try to decode
        # but ignore any error
        unless failure.encoded_attributes.nil?
          begin
            attrs = converter.from_payload(failure.encoded_attributes)
            if attrs.is_a?(Hash)
              # Shallow dup failure here to avoid affecting caller
              failure = failure.dup
              failure.message = attrs['message'] if attrs.key?('message')
              failure.stack_trace = attrs['stack_trace'] if attrs.key?('stack_trace')
            end
          rescue StandardError
            # Ignore failures
          end
        end

        # Convert
        error = if failure.application_failure_info
                  Error::ApplicationError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Application error'),
                    *converter.from_payloads(failure.application_failure_info.details),
                    type: Internal::ProtoUtils.string_or(failure.application_failure_info.type),
                    non_retryable: failure.application_failure_info.non_retryable,
                    next_retry_delay: Internal::ProtoUtils.duration_to_seconds(
                      failure.application_failure_info.next_retry_delay
                    ),
                    category: Internal::ProtoUtils.enum_to_int(Api::Enums::V1::ApplicationErrorCategory,
                                                               failure.application_failure_info.category)
                  )
                elsif failure.timeout_failure_info
                  Error::TimeoutError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Timeout'),
                    type: Internal::ProtoUtils.enum_to_int(Api::Enums::V1::TimeoutType,
                                                           failure.timeout_failure_info.timeout_type),
                    last_heartbeat_details: converter.from_payloads(
                      failure.timeout_failure_info.last_heartbeat_details
                    )
                  )
                elsif failure.canceled_failure_info
                  Error::CanceledError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Canceled'),
                    details: converter.from_payloads(failure.canceled_failure_info.details)
                  )
                elsif failure.terminated_failure_info
                  Error::TerminatedError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Terminated'),
                    details: []
                  )
                elsif failure.server_failure_info
                  Error::ServerError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Server error'),
                    non_retryable: failure.server_failure_info.non_retryable
                  )
                elsif failure.activity_failure_info
                  Error::ActivityError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Activity error'),
                    scheduled_event_id: failure.activity_failure_info.scheduled_event_id,
                    started_event_id: failure.activity_failure_info.started_event_id,
                    identity: failure.activity_failure_info.identity,
                    activity_type: failure.activity_failure_info.activity_type.name,
                    activity_id: failure.activity_failure_info.activity_id,
                    retry_state: Internal::ProtoUtils.enum_to_int(
                      Api::Enums::V1::RetryState,
                      failure.activity_failure_info.retry_state,
                      zero_means_nil: true
                    )
                  )
                elsif failure.child_workflow_execution_failure_info
                  Error::ChildWorkflowError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Child workflow error'),
                    namespace: failure.child_workflow_execution_failure_info.namespace,
                    workflow_id: failure.child_workflow_execution_failure_info.workflow_execution.workflow_id,
                    run_id: failure.child_workflow_execution_failure_info.workflow_execution.run_id,
                    workflow_type: failure.child_workflow_execution_failure_info.workflow_type.name,
                    initiated_event_id: failure.child_workflow_execution_failure_info.initiated_event_id,
                    started_event_id: failure.child_workflow_execution_failure_info.started_event_id,
                    retry_state: Internal::ProtoUtils.enum_to_int(
                      Api::Enums::V1::RetryState,
                      failure.child_workflow_execution_failure_info.retry_state,
                      zero_means_nil: true
                    )
                  )
                elsif failure.nexus_operation_execution_failure_info
                  info = failure.nexus_operation_execution_failure_info
                  token = info.operation_token
                  Error::NexusOperationError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Nexus operation error'),
                    endpoint: info.endpoint,
                    service: info.service,
                    operation: info.operation,
                    operation_token: token.empty? ? nil : token,
                    scheduled_event_id: info.scheduled_event_id,
                    original_failure: failure
                  )
                elsif failure.nexus_handler_failure_info
                  info = failure.nexus_handler_failure_info
                  type_sym = info.type.downcase.to_sym
                  unless NexusRPC::HandlerErrorType::ALL.include?(type_sym)
                    type_sym = NexusRPC::HandlerErrorType::INTERNAL
                  end

                  retry_behavior_int = Internal::ProtoUtils.enum_to_int(
                    Api::Enums::V1::NexusHandlerErrorRetryBehavior,
                    info.retry_behavior
                  )
                  retryable = _retry_behavior_to_retryable_override(retry_behavior_int)

                  NexusRPC::HandlerError.new(
                    Internal::ProtoUtils.string_or(failure.message, 'Nexus handler error'),
                    type: type_sym,
                    retryable: retryable,
                    failure: _temporal_failure_to_nexus_failure(failure)
                  )
                else
                  Error::Failure.new(Internal::ProtoUtils.string_or(failure.message, 'Failure error'))
                end

        Error._with_backtrace_and_cause(
          error,
          backtrace: failure.stack_trace.split("\n"),
          cause: failure.cause ? from_failure(failure.cause, converter) : nil
        )
      end

      private

      # Convert a Temporal failure proto to a NexusRPC::Failure for storage on NexusRPC::HandlerError.
      # This enables round-tripping: the full temporal failure can be reconstituted later.
      def _temporal_failure_to_nexus_failure(failure)
        # Deep clone to avoid mutating the input
        f = Api::Failure::V1::Failure.decode(Api::Failure::V1::Failure.encode(failure))
        message = f.message
        stack_trace = f.stack_trace
        f.message = ''
        f.stack_trace = ''

        json_str = Api::Failure::V1::Failure.encode_json(f, emit_defaults: false)
        details_hash = JSON.parse(json_str)
        details_hash.delete('message')
        details_hash.delete('stackTrace')
        details_hash.delete_if { |_, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        NexusRPC::Failure.new(
          message: message,
          stack_trace: stack_trace.empty? ? nil : stack_trace,
          metadata: { 'type' => TEMPORAL_FAILURE_PROTO_TYPE },
          details: details_hash.empty? ? nil : details_hash
        )
      end

      # Reconstitute a Temporal failure proto from a NexusRPC::Failure.
      def _nexus_failure_to_temporal_failure(nexus_failure, retryable)
        failure = Api::Failure::V1::Failure.new

        if nexus_failure.metadata&.dig('type') == TEMPORAL_FAILURE_PROTO_TYPE && nexus_failure.details
          failure = Api::Failure::V1::Failure.decode_json(JSON.generate(nexus_failure.details))
        else
          failure.application_failure_info = Api::Failure::V1::ApplicationFailureInfo.new(
            type: 'NexusFailure',
            non_retryable: !retryable
          )
        end

        failure.message = nexus_failure.message || ''
        failure.stack_trace = nexus_failure.stack_trace || ''
        failure
      end

      def _retryable_override_to_retry_behavior(retryable_override)
        if retryable_override == true
          Api::Enums::V1::NexusHandlerErrorRetryBehavior::NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
        elsif retryable_override == false
          Api::Enums::V1::NexusHandlerErrorRetryBehavior::NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
        else
          Api::Enums::V1::NexusHandlerErrorRetryBehavior::NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
        end
      end

      def _retry_behavior_to_retryable_override(retry_behavior_int)
        if retry_behavior_int ==
           Api::Enums::V1::NexusHandlerErrorRetryBehavior::NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
          true
        elsif retry_behavior_int ==
              Api::Enums::V1::NexusHandlerErrorRetryBehavior::NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
          false
        end
      end
    end
  end
end
