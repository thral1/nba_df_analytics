# Executable Scripts

This directory contains the main executable Ruby scripts for the NBA Analytics pipeline.

## Scripts

### collect_data.rb
Downloads NBA averages and box score data from NBA.com API for a specified date range.

**Usage**:
```bash
ruby bin/collect_data.rb --season 2023-24 --start-date 2023-10-24
```

### parse_stats.rb
Parses raw NBA statistics and performs feature engineering to create ML-ready datasets.

**Usage**:
```bash
ruby bin/parse_stats.rb --season 2023-24 --output data/processed/
```

### helpers.rb
Shared helper functions used across multiple scripts. Not meant to be executed directly.

## Note

These scripts require the Ruby environment to be properly configured with all dependencies installed:

```bash
bundle install
```

See the main [README.md](../README.md) for complete setup instructions.
