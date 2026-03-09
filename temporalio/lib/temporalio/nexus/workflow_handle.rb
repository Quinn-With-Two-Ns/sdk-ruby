# frozen_string_literal: true

require 'base64'
require 'json'

module Temporalio
  module Nexus
    # A handle to a workflow that is backing a Nexus operation.
    #
    # WARNING: Nexus support is experimental.
    #
    # Do not instantiate this directly. Obtain a handle via the nexus worker context when starting a workflow-backed
    # operation.
    WorkflowHandle = Data.define(:namespace, :workflow_id) do
      # Operation token type constant for workflow-backed handles.
      OPERATION_TOKEN_TYPE_WORKFLOW = 1

      # Convert this handle to a base64url-encoded token string.
      #
      # @return [String] Token string that can be passed back via {from_token}.
      def to_token
        payload = JSON.generate({ 't' => OPERATION_TOKEN_TYPE_WORKFLOW, 'ns' => namespace, 'wid' => workflow_id })
        Base64.urlsafe_encode64(payload, padding: false)
      end

      # Decode and validate a token string, returning a {WorkflowHandle}.
      #
      # @param token [String] Base64url-encoded token string.
      # @return [WorkflowHandle]
      # @raise [TypeError] If the token is invalid.
      def self.from_token(token)
        raise TypeError, 'invalid workflow token: token is empty' if token.nil? || token.empty?

        # Validate base64url characters
        unless token.match?(/\A[A-Za-z0-9_\-]*\z/)
          raise TypeError, 'failed to decode token as base64url: invalid characters'
        end

        # Add padding back for Ruby's Base64 decoder
        padded = token + ('=' * ((-token.length) % 4))
        begin
          decoded = Base64.urlsafe_decode64(padded)
        rescue ArgumentError => e
          raise TypeError, "failed to decode token as base64url: #{e.message}"
        end

        begin
          parsed = JSON.parse(decoded)
        rescue JSON::ParserError => e
          raise TypeError, "failed to unmarshal workflow operation token: #{e.message}"
        end

        unless parsed.is_a?(Hash)
          raise TypeError, "invalid workflow token: expected Hash, got #{parsed.class}"
        end

        token_type = parsed['t']
        unless token_type == OPERATION_TOKEN_TYPE_WORKFLOW
          raise TypeError, "invalid workflow token type: #{token_type.inspect}, expected: #{OPERATION_TOKEN_TYPE_WORKFLOW}"
        end

        version = parsed['v']
        if !version.nil? && version != 0
          raise TypeError, "invalid workflow token: 'v' field, if present, must be 0 or null/absent"
        end

        workflow_id = parsed['wid']
        unless workflow_id.is_a?(String) && !workflow_id.empty?
          raise TypeError, 'invalid workflow token: missing, empty, or non-string workflow ID (wid)'
        end

        namespace = parsed['ns']
        unless namespace.is_a?(String)
          raise TypeError, 'invalid workflow token: missing or non-string namespace (ns)'
        end

        new(namespace: namespace, workflow_id: workflow_id)
      end
    end
  end
end
