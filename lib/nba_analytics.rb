# Main entry point for NBA Analytics library

require_relative 'utils/config'

module NBAAnalytics
  VERSION = '1.0.0'
end

# Auto-require common dependencies
require 'sequel'
require 'csv'
require 'date'
require 'json'
require 'nokogiri'
require 'optimist'
