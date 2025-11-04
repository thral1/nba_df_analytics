source 'https://rubygems.org'

ruby '>= 3.0.0'

# Database
gem 'sequel', '~> 5.72'
gem 'sqlite3', '~> 1.6'

# Web scraping and parsing
gem 'nokogiri', '~> 1.15'

# Command line
gem 'optimist', '~> 3.1'  # Modern replacement for trollop

# Utilities
gem 'ruby-duration', '~> 3.2'

group :development do
  gem 'pry', '~> 0.14'
  gem 'pry-byebug', '~> 3.10'
end

group :test do
  gem 'rspec', '~> 3.12'
  gem 'rspec-its', '~> 1.3'
  gem 'simplecov', '~> 0.22', require: false
  gem 'factory_bot', '~> 6.2'
  gem 'faker', '~> 3.2'
end

group :development, :test do
  gem 'rubocop', '~> 1.56', require: false
  gem 'rubocop-rspec', '~> 2.24', require: false
end
