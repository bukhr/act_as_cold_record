# frozen_string_literal: true
require "active_support/lazy_load_hooks"

# frozen_string_literal: true
require 'active_record'

module ActAsColdRecord
  class Railtie < Rails::Railtie
    initializer 'act_as_cold_record.insert_into_active_record' do
      ActiveSupport.on_load(:active_record) do
        require "act_as_cold_record/base"
        ActiveRecord::Base.include ::ActAsColdRecord::Base
      end
    end
  end
end
