# frozen_string_literal: true

module Temporalio
  module Nexus
    # Information about a running Nexus operation, accessible via {Nexus.info}.
    #
    # WARNING: Nexus support is experimental.
    Info = Data.define(:task_queue, :service, :operation)
  end
end
