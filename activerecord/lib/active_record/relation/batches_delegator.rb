module ActiveRecord
  module Batches
    class BatchesDelegator
      include Enumerable
      def initialize(of: 1000, begin_at: nil, end_at: nil, relation:)
        @of       = of
        @relation = relation
        @begin_at = begin_at
        @end_at   = end_at
      end

      # Iterating through the records, e.g.:
      #
      #   People.in_batches_delegator.each_record(&:party_all_night!)
      #
      # Iterating through the relations:
      #
      #   People.in_batches_delegator
      def each_record
        enum = @relation.to_enum(:find_each, batch_size: @of, begin_at: @begin_at, end_at: @end_at)
        return enum.map { |record| yield record } if block_given?
        enum
      end

      def each
        enum = @relation.to_enum(:in_batches, of: @of, begin_at: @begin_at, end_at: @end_at, load: false)
        return enum.map { |relation| yield relation } if block_given?
        enum
      end

      private

      def self.delegate(*methods)
        options = {load: false}
        options = methods.pop if methods.last.is_a?(Hash)

        methods.each do |method|
          define_method(method) do |*args|
            @relation.to_enum(:in_batches, of: @of, begin_at: @begin_at, end_at: @end_at, load: options[:load]).map do |relation|
              relation.send(method, *args)
            end
          end
        end
      end

      # Examples usage:
      #
      #   People.where('age > 21').in_batches_delegator.update_all('age = age + 1')
      #   People.in_batches_delegator.update_all('age = age + 1')
      #   People.in_batches_delegator.delete_all
      #
      delegate :destroy, :destroy_all, :delete, :delete_all, :update, :update_all, load: false
    end
  end
end
