module NBAAnalytics
  module Utils
    module MathHelpers
      module_function

      # Safely divide two numbers, returning 0.0 if denominator is zero or either value is nil
      def divide(num, den)
        return 0.0 if num.nil? || den.nil? || den.zero?
        num.to_f / den.to_f
      end

      # Calculate median value from a hash of values
      def median(hash)
        array = hash.sort_by { |_k, v| v }
        len = array.length
        return 0.0 if len.zero?

        center = len / 2
        if len.odd?
          array[center][1].to_f
        else
          (array[center][1].to_f + array[center - 1][1].to_f) / 2.0
        end
      end

      # Calculate mean, updating total and returning both total and mean
      def mean(value, total, games_played)
        total += value.to_f
        avg = games_played.zero? ? 0.0 : (total / games_played).round(3)
        [total, avg]
      end
    end
  end
end
