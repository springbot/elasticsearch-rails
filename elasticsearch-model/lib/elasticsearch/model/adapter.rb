module Elasticsearch
  module Model

    # Contains an adapter which provides OxM-specific implementations for common behaviour:
    #
    # * {Adapter::Adapter#records_mixin   Fetching records from the database}
    # * {Adapter::Adapter#callbacks_mixin Model callbacks for automatic index updates}
    # * {Adapter::Adapter#importing_mixin Efficient bulk loading from the database}
    #
    # @see Elasticsearch::Model::Adapter::Default
    # @see Elasticsearch::Model::Adapter::ActiveRecord
    # @see Elasticsearch::Model::Adapter::Mongoid
    #
    module Adapter

      # Returns an adapter based on the Ruby class passed
      #
      # @example Create an adapter for an ActiveRecord-based model
      #
      #     class Article < ActiveRecord::Base; end
      #
      #     myadapter = Elasticsearch::Model::Adapter.from_class(Article)
      #     myadapter.adapter
      #     # => Elasticsearch::Model::Adapter::ActiveRecord
      #
      # @see Adapter.adapters The list of included adapters
      # @see Adapter.register Register a custom adapter
      #
      def from_class(klass)
        Adapter.new(klass)
      end; module_function :from_class

      # Returns registered adapters
      #
      # @see ::Elasticsearch::Model::Adapter::Adapter.adapters
      #
      def adapters
        Adapter.adapters
      end; module_function :adapters

      # Registers an adapter
      #
      # @see ::Elasticsearch::Model::Adapter::Adapter.register
      #
      def register(name, condition)
        Adapter.register(name, condition)
      end; module_function :register

      # Pushes a registration to the beginning of execution loop
      #
      # @see ::Elasticsearch::Model::Adapter::Adapter.push_registration
      #
      def push_registration(name, condition)
        Adapter.push_registration(name, condition)
      end; module_function :push_registration

      # Contains an adapter for specific OxM or architecture.
      #
      class Adapter
        attr_reader :klass

        def initialize(klass)
          @klass = klass
        end

        # Registers an adapter for specific condition
        #
        # @param name      [Module] The module containing the implemented interface
        # @param condition [Proc]   An object with a `call` method which is evaluated in {.adapter}
        #
        # @example Register an adapter for DataMapper
        #
        #     module DataMapperAdapter
        #
        #       # Implement the interface for fetching records
        #       #
        #       module Records
        #         def records
        #           klass.all(id: @ids)
        #         end
        #
        #         # ...
        #       end
        #     end
        #
        #     # Register the adapter
        #     #
        #     Elasticsearch::Model::Adapter.register(
        #       DataMapperAdapter,
        #       lambda { |klass|
        #         defined?(::DataMapper::Resource) and klass.ancestors.include?(::DataMapper::Resource)
        #       }
        #     )
        #
        def self.register(name, condition)
          self.adapters[name] = condition
        end

        # Pushes an adapter for a condition so its lambda exectues first.
        # Useful for overriding the behavior of a default adapter on a class by
        # class basis.
        def self.push_registration(name, condition)
          @adapters = Hash[name, condition].merge self.adapters
        end

        # Return the collection of registered adapters
        #
        # @example Return the currently registered adapters
        #
        #     Elasticsearch::Model::Adapter.adapters
        #     # => {
        #     #  Elasticsearch::Model::Adapter::ActiveRecord => #<Proc:0x007...(lambda)>,
        #     #  Elasticsearch::Model::Adapter::Mongoid => #<Proc:0x007... (lambda)>,
        #     # }
        #
        # @return [Hash] The collection of adapters
        #
        def self.adapters
          @adapters ||= {}
        end

        # Return the module with {Default::Records} interface implementation
        #
        # @api private
        #
        def records_mixin
          adapter.const_get(:Records)
        end

        # Return the module with {Default::Callbacks} interface implementation
        #
        # @api private
        #
        def callbacks_mixin
          adapter.const_get(:Callbacks)
        end

        # Return the module with {Default::Importing} interface implementation
        #
        # @api private
        #
        def importing_mixin
          adapter.const_get(:Importing)
        end

        # Returns the adapter module
        #
        # @api private
        #
        def adapter
          @adapter ||= begin
            self.class.adapters.find( lambda {[]} ) { |name, condition| condition.call(klass) }.first \
            || Elasticsearch::Model::Adapter::Default
          end
        end

      end
    end
  end
end
