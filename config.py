# config.py
{
    "data_paths": {
        "raw_dir": "data/raw",
        "processed_dir": "data/processed",
        "sentiment_dir": "data/sentiment",
        "trend_dir": "data/trends"
    },
    "yelp_api": {
        "endpoint": "https://api.yelp.com/v3",
        "location": "Boston, MA",
        "categories": "restaurants",
        "limit": 50
    },
    "sentiment_model": {
        "model_name": "roberta-base",
        "aspects": ["service", "food", "ambiance"]
    },
    "snowflake_config_path": "config/snowflake.json"
}