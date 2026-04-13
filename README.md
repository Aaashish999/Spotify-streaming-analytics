# Spotify Streaming Analytics

End-to-end music streaming analytics pipeline built with dbt and Snowflake.
Transforms raw streaming events into a star schema with fact and dimension
tables, delivering insights on top artists, user engagement, genre trends,
and daily listening patterns.

## Tech Stack

| Tool | Purpose |
|------|---------|
| Snowflake | Cloud data warehouse |
| dbt Core 1.7 | Data transformation |
| SQL | Data modeling |
| Git + GitHub | Version control |

## Project Architecture
| Model | Type | Description |
|-------|------|-------------|
| `stg_streams` | View | Cleaned streaming events |
| `stg_users` | View | Cleaned user data |
| `stg_artists` | View | Cleaned artist data |
| `fct_streams` | Table | Core fact table — one row per stream |
| `dim_users` | Table | User attributes and segmentation |
| `dim_artists` | Table | Artist metadata and tier |
| `agg_top_artists` | Table | Top artists by streams and listening time |
| `agg_user_listening_time` | Table | User engagement metrics |
| `agg_genre_popularity` | Table | Monthly genre trends |
| `agg_daily_trends` | Table | Daily volume with 7-day rolling average |

## Key Metrics

- Top artists by total streams and listening time
- User engagement — completion rates and listener tiers
- Genre popularity trends month over month
- Daily streaming volume with 7-day rolling average

## Sample Insights

- Bad Bunny and Taylor Swift are the most streamed artists
- Pop accounts for the largest share of streams
- Premium users show higher completion rates than free users
- 7-day rolling average shows consistent engagement growth

## Setup
```bash
# Clone the repo
git clone https://github.com/Aaashish999/Spotify-streaming-analytics.git
cd Spotify-streaming-analytics

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dbt
pip install dbt-core==1.7.9 dbt-snowflake==1.7.1

# Configure Snowflake credentials in ~/.dbt/profiles.yml

# Load data
dbt seed

# Run all models
dbt run

# Run tests
dbt test
```

## dbt Lineage

## Testing

18 dbt tests covering:
- Primary key uniqueness
- Not null constraints
- Accepted values validation
- Referential integrity
