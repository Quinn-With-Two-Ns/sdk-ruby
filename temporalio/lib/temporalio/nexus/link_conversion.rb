# frozen_string_literal: true

require 'uri'
require 'temporalio/api/common/v1/message'
require 'temporalio/api/enums/v1/event_type'

module Temporalio
  module Nexus
    # Utilities for converting between Nexus link URLs and WorkflowEvent proto link objects.
    #
    # WARNING: Nexus support is experimental.
    module LinkConversion
      module_function

      LINK_URL_PATH_REGEX = %r{\A/namespaces/(?<namespace>[^/]+)/workflows/(?<workflow_id>[^/]+)/(?<run_id>[^/]+)/history\z}
      LINK_EVENT_ID_PARAM_NAME = 'eventID'
      LINK_EVENT_TYPE_PARAM_NAME = 'eventType'
      LINK_REQUEST_ID_PARAM_NAME = 'requestID'
      LINK_REFERENCE_TYPE_PARAM_NAME = 'referenceType'
      EVENT_REFERENCE_TYPE = 'EventReference'
      REQUEST_ID_REFERENCE_TYPE = 'RequestIdReference'

      # Convert a Nexus link (with a +temporal:///+ URL) into a +Link::WorkflowEvent+ proto.
      #
      # @param link [#url] A Nexus link object with a +url+ attribute.
      # @return [Api::Common::V1::Link::WorkflowEvent, nil] The parsed workflow event link, or +nil+ on parse failure.
      def nexus_link_to_workflow_event(link)
        uri = URI.parse(link.url)
        match = LINK_URL_PATH_REGEX.match(uri.path)
        unless match
          warn "Invalid Nexus link: #{link.url}. Expected path to match #{LINK_URL_PATH_REGEX.source}"
          return nil
        end

        query_params = URI.decode_www_form(uri.query.to_s).each_with_object({}) do |(k, v), h|
          (h[k] ||= []) << v
        end

        reference_type = (query_params[LINK_REFERENCE_TYPE_PARAM_NAME] || []).first
        event_ref = nil
        request_id_ref = nil

        case reference_type
        when EVENT_REFERENCE_TYPE
          event_ref = query_params_to_event_reference(query_params)
          return nil if event_ref.nil?
        when REQUEST_ID_REFERENCE_TYPE
          request_id_ref = query_params_to_request_id_reference(query_params)
          return nil if request_id_ref.nil?
        else
          warn "Invalid Nexus link: #{link.url}. Expected #{LINK_REFERENCE_TYPE_PARAM_NAME} to be '#{EVENT_REFERENCE_TYPE}' or '#{REQUEST_ID_REFERENCE_TYPE}'"
          return nil
        end

        Api::Common::V1::Link::WorkflowEvent.new(
          namespace: URI.decode_uri_component(match[:namespace]),
          workflow_id: URI.decode_uri_component(match[:workflow_id]),
          run_id: URI.decode_uri_component(match[:run_id]),
          event_ref: event_ref,
          request_id_ref: request_id_ref
        )
      rescue StandardError => e
        warn "Failed to parse Nexus link: #{link.url}: #{e.message}"
        nil
      end

      # Convert a +Link::WorkflowEvent+ proto into a Nexus link URL string.
      #
      # @param workflow_event [Api::Common::V1::Link::WorkflowEvent]
      # @return [String] The +temporal:///+ URL for the link.
      def workflow_event_to_nexus_link(workflow_event)
        namespace = URI.encode_uri_component(workflow_event.namespace)
        workflow_id = URI.encode_uri_component(workflow_event.workflow_id)
        run_id = URI.encode_uri_component(workflow_event.run_id)
        path = "/namespaces/#{namespace}/workflows/#{workflow_id}/#{run_id}/history"

        query = case workflow_event.reference
                when :event_ref
                  event_reference_to_query_params(workflow_event.event_ref)
                when :request_id_ref
                  request_id_reference_to_query_params(workflow_event.request_id_ref)
                end

        url = "temporal://#{path}"
        url = "#{url}?#{query}" if query && !query.empty?
        url
      end

      # @!visibility private
      def event_reference_to_query_params(event_ref)
        event_type_name = event_type_value_to_name(event_ref.event_type)
        URI.encode_www_form(
          LINK_EVENT_ID_PARAM_NAME => event_ref.event_id,
          LINK_EVENT_TYPE_PARAM_NAME => event_type_name,
          LINK_REFERENCE_TYPE_PARAM_NAME => EVENT_REFERENCE_TYPE
        )
      end

      # @!visibility private
      def request_id_reference_to_query_params(request_id_ref)
        event_type_name = event_type_value_to_name(request_id_ref.event_type)
        params = { LINK_REFERENCE_TYPE_PARAM_NAME => REQUEST_ID_REFERENCE_TYPE }
        params[LINK_REQUEST_ID_PARAM_NAME] = request_id_ref.request_id if request_id_ref.request_id && !request_id_ref.request_id.empty?
        params[LINK_EVENT_TYPE_PARAM_NAME] = event_type_name
        URI.encode_www_form(params)
      end

      # @!visibility private
      def query_params_to_event_reference(query_params)
        raw_event_type = (query_params[LINK_EVENT_TYPE_PARAM_NAME] || []).first
        if raw_event_type.nil?
          warn "query params do not contain event type: #{query_params.inspect}"
          return nil
        end

        event_type_int = parse_event_type_name(raw_event_type)
        if event_type_int.nil?
          warn "Invalid event type name: #{raw_event_type.inspect}"
          return nil
        end

        raw_event_id = (query_params[LINK_EVENT_ID_PARAM_NAME] || []).first
        event_id = 0
        if raw_event_id && !raw_event_id.empty?
          begin
            event_id = Integer(raw_event_id)
          rescue ArgumentError
            warn "Query params contain invalid event id: #{raw_event_id.inspect}"
            return nil
          end
        end

        Api::Common::V1::Link::WorkflowEvent::EventReference.new(
          event_type: event_type_int,
          event_id: event_id
        )
      end

      # @!visibility private
      def query_params_to_request_id_reference(query_params)
        raw_event_type = (query_params[LINK_EVENT_TYPE_PARAM_NAME] || []).first
        if raw_event_type.nil?
          warn "query params do not contain event type: #{query_params.inspect}"
          return nil
        end

        event_type_int = parse_event_type_name(raw_event_type)
        if event_type_int.nil?
          warn "Invalid event type name: #{raw_event_type.inspect}"
          return nil
        end

        request_id = (query_params[LINK_REQUEST_ID_PARAM_NAME] || []).first || ''

        Api::Common::V1::Link::WorkflowEvent::RequestIdReference.new(
          request_id: request_id,
          event_type: event_type_int
        )
      end

      # @!visibility private
      # Convert an event_type value (integer or symbol) to the PascalCase name used in link URLs.
      def event_type_value_to_name(event_type)
        # event_type may be an integer or a symbol like :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        sym = if event_type.is_a?(Symbol)
                event_type
              else
                Api::Enums::V1::EventType.descriptor.lookup_value(event_type)
              end
        name = sym.to_s
        name = name.sub(/\AEVENT_TYPE_/, '') if name.start_with?('EVENT_TYPE_')
        event_type_constant_case_to_pascal_case(name)
      end

      # @!visibility private
      # Parse an event type name (either CONSTANT_CASE or PascalCase) to an integer.
      def parse_event_type_name(name)
        constant_name = if name.start_with?('EVENT_TYPE_')
                          name.to_sym
                        elsif name.match?(/\A[A-Z][a-z]/)
                          :"EVENT_TYPE_#{event_type_pascal_case_to_constant_case(name)}"
                        else
                          return nil
                        end
        Api::Enums::V1::EventType.descriptor.lookup_name(constant_name)
      end

      # @!visibility private
      # Convert CONSTANT_CASE to PascalCase (e.g. "NEXUS_OPERATION_SCHEDULED" -> "NexusOperationScheduled").
      def event_type_constant_case_to_pascal_case(str)
        str.downcase.split('_').map(&:capitalize).join
      end

      # @!visibility private
      # Convert PascalCase to CONSTANT_CASE (e.g. "NexusOperationScheduled" -> "NEXUS_OPERATION_SCHEDULED").
      def event_type_pascal_case_to_constant_case(str)
        str.gsub(/([A-Z])/) { "_#{$1}" }.sub(/\A_/, '').upcase
      end
    end
  end
end
