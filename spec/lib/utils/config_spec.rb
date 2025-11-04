require 'spec_helper'

RSpec.describe NBAAnalytics::Config do
  describe '.seasons' do
    it 'loads seasons from YAML file' do
      seasons = described_class.seasons
      expect(seasons).to be_a(Hash)
      expect(seasons).to have_key('2023-24')
    end

    it 'includes season dates' do
      season = described_class.seasons['2023-24']
      expect(season).to have_key('regular_season_start')
      expect(season).to have_key('regular_season_end')
      expect(season).to have_key('playoffs_end')
    end

    it 'caches the seasons hash' do
      first_call = described_class.seasons
      second_call = described_class.seasons
      expect(first_call.object_id).to eq(second_call.object_id)
    end
  end

  describe '.database_path' do
    it 'returns default path when env var not set' do
      allow(ENV).to receive(:[]).with('DB_PATH').and_return(nil)
      expect(described_class.database_path).to eq('data/processed/nba.db')
    end

    it 'returns env var value when set' do
      allow(ENV).to receive(:[]).with('DB_PATH').and_return('/custom/path.db')
      expect(described_class.database_path).to eq('/custom/path.db')
    end
  end

  describe '.feature_output_path' do
    it 'returns default path when env var not set' do
      allow(ENV).to receive(:[]).with('FEATURE_OUTPUT_PATH').and_return(nil)
      expect(described_class.feature_output_path).to eq('output/features/')
    end
  end

  describe '.log_level' do
    it 'returns default log level' do
      allow(ENV).to receive(:[]).with('LOG_LEVEL').and_return(nil)
      expect(described_class.log_level).to eq('INFO')
    end
  end
end
