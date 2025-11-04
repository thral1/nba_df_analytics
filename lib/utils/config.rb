require 'yaml'

module NBAAnalytics
  class Config
    class << self
      def seasons
        @seasons ||= YAML.load_file(seasons_path)['seasons']
      end

      def database_path
        ENV['DB_PATH'] || 'data/processed/nba.db'
      end

      def feature_output_path
        ENV['FEATURE_OUTPUT_PATH'] || 'output/features/'
      end

      def log_path
        ENV['LOG_PATH'] || 'logs/nba_analytics.log'
      end

      def log_level
        ENV['LOG_LEVEL'] || 'INFO'
      end

      private

      def seasons_path
        File.join(root_path, 'config', 'seasons.yml')
      end

      def root_path
        File.expand_path('../..', __dir__)
      end
    end
  end
end
