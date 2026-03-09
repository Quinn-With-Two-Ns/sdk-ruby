# frozen_string_literal: true

module Temporalio
  module Internal
    module Worker
      # Internal context for a running Nexus operation. Not exposed to users.
      #
      # @!visibility private
      class NexusOperationContext
        attr_reader :client, :task_queue, :data_converter, :metric_meter,
                    :inbound_links, :outbound_links, :callback_url,
                    :callback_headers, :request_id, :info,
                    :worker_shutdown_cancellation

        def initialize(
          client:,
          task_queue:,
          data_converter:,
          metric_meter:,
          inbound_links:,
          outbound_links:,
          callback_url:,
          callback_headers:,
          request_id:,
          info:,
          worker_shutdown_cancellation:
        )
          @client = client
          @task_queue = task_queue
          @data_converter = data_converter
          @metric_meter = metric_meter
          @inbound_links = inbound_links
          @outbound_links = outbound_links
          @callback_url = callback_url
          @callback_headers = callback_headers
          @request_id = request_id
          @info = info
          @worker_shutdown_cancellation = worker_shutdown_cancellation
        end

        # Get the current operation context.
        #
        # @return [NexusOperationContext]
        # @raise [RuntimeError] If not currently in a Nexus operation.
        def self.current
          ctx = _current_raw
          raise 'Not in Nexus operation context' if ctx.nil?

          ctx
        end

        # Get the current operation context, or nil if not in a Nexus operation.
        #
        # @return [NexusOperationContext, nil]
        def self.current_or_nil
          _current_raw
        end

        # @!visibility private
        def self._current_raw
          if Fiber.current_scheduler
            Fiber[:temporal_nexus_context]
          else
            Thread.current[:temporal_nexus_context]
          end
        end

        # @!visibility private
        def self._current_raw=(ctx)
          if Fiber.current_scheduler
            Fiber[:temporal_nexus_context] = ctx
          else
            Thread.current[:temporal_nexus_context] = ctx
          end
        end
      end
    end
  end
end
