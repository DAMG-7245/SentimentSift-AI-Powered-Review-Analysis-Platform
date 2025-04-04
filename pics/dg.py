from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.onprem.workflow import Airflow
from diagrams.programming.framework import FastAPI
from diagrams.custom import Custom
from diagrams.onprem.client import User

with Diagram("Simplified SentimentSift Architecture", show=True, filename="simplified_sentimentsift_architecture", direction="LR"):

    # Users
    user = User("End Users")

    # Data Sources
    with Cluster("Data Sources"):
        google_places = Custom("Google Places API", "./icons/Google.png")
        web_scraping = Custom("Web Scraping", "./icons/python.png")
        social_media = Custom("Social Media", "./icons/social.png")

    # ETL Pipeline
    with Cluster("ETL Pipeline"):
        airflow = Airflow("Airflow")
        data_processing = Custom("Data Processing", "./icons/python.png")

    # Storage
    s3 = S3("AWS S3\n(Storage)")

    # Backend
    with Cluster("Backend"):
        fastapi = FastAPI("FastAPI Service")
        llm = Custom("LLM Services", "./icons/llm.png")
        agent_system = Custom("Multi-Agent System", "./icons/python.png")

    # Frontend
    frontend = Custom("Streamlit UI", "./icons/streamlit.png")

    # Diagram connections
    user >> frontend
    frontend >> fastapi

    # Data sources to ETL
    google_places >> airflow
    web_scraping >> airflow
    social_media >> airflow

    airflow >> data_processing >> s3

    # Storage to Backend
    s3 >> fastapi >> llm >> agent_system

    # Backend to Frontend
    fastapi >> frontend
