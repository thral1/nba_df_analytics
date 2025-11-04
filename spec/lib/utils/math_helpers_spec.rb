require 'spec_helper'

RSpec.describe NBAAnalytics::Utils::MathHelpers do
  describe '.divide' do
    it 'divides two numbers correctly' do
      expect(described_class.divide(10, 2)).to eq(5.0)
    end

    it 'handles division by zero' do
      expect(described_class.divide(10, 0)).to eq(0.0)
    end

    it 'handles nil numerator' do
      expect(described_class.divide(nil, 2)).to eq(0.0)
    end

    it 'handles nil denominator' do
      expect(described_class.divide(10, nil)).to eq(0.0)
    end

    it 'handles both nil values' do
      expect(described_class.divide(nil, nil)).to eq(0.0)
    end

    it 'converts integers to floats' do
      result = described_class.divide(7, 2)
      expect(result).to eq(3.5)
      expect(result).to be_a(Float)
    end
  end

  describe '.median' do
    it 'calculates median of odd-length hash' do
      hash = { a: 1, b: 3, c: 5 }
      expect(described_class.median(hash)).to eq(3.0)
    end

    it 'calculates median of even-length hash' do
      hash = { a: 1, b: 2, c: 4, d: 5 }
      expect(described_class.median(hash)).to eq(3.0)
    end

    it 'handles single element' do
      hash = { a: 42 }
      expect(described_class.median(hash)).to eq(42.0)
    end

    it 'handles empty hash' do
      expect(described_class.median({})).to eq(0.0)
    end

    it 'handles unsorted hash' do
      hash = { a: 10, b: 1, c: 5 }
      expect(described_class.median(hash)).to eq(5.0)
    end
  end

  describe '.mean' do
    it 'calculates mean correctly' do
      total, avg = described_class.mean(10.0, 20.0, 3)
      expect(total).to eq(30.0)
      expect(avg).to eq(10.0)
    end

    it 'handles zero games played' do
      total, avg = described_class.mean(10.0, 0.0, 0)
      expect(total).to eq(10.0)
      expect(avg).to eq(0.0)
    end

    it 'rounds to 3 decimal places' do
      total, avg = described_class.mean(10.0, 20.0, 9)
      expect(avg).to eq(3.333)
    end

    it 'converts values to floats' do
      total, avg = described_class.mean(10, 20, 3)
      expect(total).to be_a(Float)
      expect(avg).to be_a(Float)
    end
  end
end
