# frozen_string_literal: true

require 'temporalio/nexus/link_conversion'
require 'temporalio/nexus/operation_context'
require 'temporalio/nexus/workflow_handle'
require 'temporalio/nexus/workflow_run_operation_handler'

module Temporalio
  # All Nexus-related classes.
  #
  # WARNING: Nexus support is experimental.
  module Nexus
    # Get the Temporal client for the current Nexus operation.
    #
    # @return [Client] The current client.
    # @raise [RuntimeError] If not currently in a Nexus operation.
    def self.client
      Internal::Worker::NexusOperationContext.current.client
    end

    # Get information about the current Nexus operation.
    #
    # @return [Info] The current operation info.
    # @raise [RuntimeError] If not currently in a Nexus operation.
    def self.info
      Internal::Worker::NexusOperationContext.current.info
    end

    # Whether the current code is executing inside a Nexus operation handler.
    #
    # @return [Boolean]
    def self.in_operation?
      !Internal::Worker::NexusOperationContext.current_or_nil.nil?
    end

    # Get the metric meter for the current Nexus operation.
    #
    # @return [Metric::Meter] The current metric meter.
    # @raise [RuntimeError] If not currently in a Nexus operation.
    def self.metric_meter
      Internal::Worker::NexusOperationContext.current.metric_meter
    end

    # Whether the worker is shutting down.
    #
    # @return [Boolean] True if shutdown has been initiated on the worker.
    # @raise [RuntimeError] If not currently in a Nexus operation.
    def self.worker_shutdown?
      Internal::Worker::NexusOperationContext.current.worker_shutdown_cancellation.canceled?
    end

    # Get the cancellation that is canceled when the worker is shutting down.
    #
    # This can be used to detect and respond to worker shutdown from within a Nexus operation handler.
    #
    # @return [Cancellation] Cancellation that is canceled on worker shutdown.
    # @raise [RuntimeError] If not currently in a Nexus operation.
    def self.worker_shutdown_cancellation
      Internal::Worker::NexusOperationContext.current.worker_shutdown_cancellation
    end
  end
end
