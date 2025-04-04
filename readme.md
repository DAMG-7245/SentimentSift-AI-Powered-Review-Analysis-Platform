# SentimentSift: AI-Powered Review Analysis Platform

An intelligent review analysis platform that uses advanced natural
language processing and sentiment analysis to filter emotional content from Google reviews,
providing users with more objective, factual, and balanced insights about businesses, products,
and services.


## 🔍 Overview

SentimentSift is an intelligent review analysis platform that uses advanced AI techniques to filter emotional bias from Google and other platform reviews. Our system provides users with more objective, factual insights about businesses by differentiating between subjective emotional content and objective assessments.

By leveraging natural language processing, sentiment analysis, and large language models, SentimentSift transforms how users interact with online reviews, creating a more reliable information ecosystem for decision-making.


## ✨ Features

- **Objective Review Filtering**: Automatically detects and filters emotional content from reviews
- **Multi-source Analysis**: Aggregates reviews from Google, Yelp, TripAdvisor, and other platforms
- **Sentiment Classification**: Analyzes review sentiment with advanced NLP techniques
- **Business Insights Dashboard**: Visual representations of review trends and sentiment analysis
- **Fact Extraction**: Identifies factual statements separate from opinions
- **Credibility Scoring**: Rates reviews based on objectivity and helpfulness
- **Summary Generation**: Creates concise, objective summaries of multiple reviews

## 🏗️ System Architecture

SentimentSift uses a modern, scalable architecture:

```
┌───────────────┐    ┌───────────────┐    ┌───────────────────┐
│ Data Sources  │───>│ Airflow ETL   │───>│ MongoDB/PostgreSQL│
└───────────────┘    └───────────────┘    └───────────────────┘
                                                   │
┌───────────────┐    ┌───────────────┐             │
│ Streamlit UI  │<───│ FastAPI       │<────────────┘
└───────────────┘    └───────────────┘
                            │
                     ┌──────┴──────┐
                     │ LLM Services│
                     └──────┬──────┘
                            │
                     ┌──────┴──────┐
                     │ Agent System│
                     └─────────────┘
```

## 📂 Project Structure

```
sentimentsift/
├── airflow/                 # Airflow DAGs and plugins
│   ├── dags/                # ETL pipeline definitions
│   └── plugins/             # Custom Airflow components
├── backend/                 # FastAPI application
│   ├── app/                 # API endpoints and business logic
│   │   ├── api/             # API route definitions
│   │   ├── core/            # Core application components
│   │   ├── models/          # Data models
│   │   └── services/        # Business logic services
│   └── tests/               # Backend tests
├── frontend/                # Streamlit application
│   ├── pages/               # Multi-page app components
│   └── components/          # Reusable UI components
├── agents/                  # Agent system components
│   ├── langraph/            # LangGraph agent definitions
│   └── crewai/              # CrewAI agent configurations
├── data/                    # Data processing scripts
│   ├── collectors/          # Data collection modules
│   └── processors/          # Data transformation modules
├── models/                  # Sentiment analysis models
├── tests/                   # Integration tests
├── scripts/                 # Utility scripts
├── docs/                    # Documentation
├── docker-compose.yml       # Docker services configuration
└── README.md                # This file
```


### Setup

1. Clone the repository
```bash
git clone https://github.com/DAMG-7245/SentimentSift-AI-Powered-Review-Analysis-Platform.git
cd sentimentsift
```

2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

5. Start the services using Docker Compose
```bash
docker-compose up -d
```


```

## 🚀 Usage

### Running the Airflow Pipeline

```bash
cd airflow
airflow standalone
```
Access the Airflow UI at http://localhost:8080

### Starting the FastAPI Backend

```bash
cd backend
uvicorn app.main:app --reload
```
The API will be available at http://localhost:8000

### Launching the Streamlit Frontend

```bash
cd frontend
streamlit run app.py
```
Access the web interface at http://localhost:8501

## 📚 API Documentation

Once the FastAPI server is running, visit http://localhost:8000/docs for interactive API documentation.

Key endpoints:
- `GET /reviews/{business_id}`: Get filtered reviews for a business
- `POST /analyze`: Submit a review for sentiment analysis
- `GET /summary/{business_id}`: Get an objective summary of all reviews
- `GET /metrics/{business_id}`: Get key metrics and insights


## 📊 Data Sources

SentimentSift collects and processes data from multiple sources:

### Structured Data
- Google Places API: Business details and associated reviews
- Public review datasets: For model training and validation
- User feedback: Stored in structured database tables

### Unstructured Data
- Web scraping: Reviews from Yelp, TripAdvisor, etc.
- PDF documents: Business reports and research papers
- Social media: Comments and posts from platforms like Twitter

## 🔄 Airflow Pipelines

Our ETL process is managed through Apache Airflow DAGs:

1. **review_collection_dag**: Schedules API calls and web scraping
2. **data_processing_dag**: Cleans and transforms the collected data
3. **sentiment_analysis_dag**: Runs batch sentiment analysis jobs
4. **reporting_dag**: Generates periodic insights and reports

## 🤖 Agent System

SentimentSift uses a multi-agent system leveraging various frameworks:

1. **Review Analysis Agent**: Processes individual reviews using LangGraph
2. **Fact Extraction Agent**: Identifies factual statements within reviews
3. **Summary Agent**: Generates concise summaries of multiple reviews
4. **Coordination Agent**: Orchestrates the workflow using CrewAI

MCP server integration enables distributed processing for improved performance.

---

## **👨‍💻 Authors**
* Sicheng Bao (@Jellysillyfish13)
* Yung Rou Ko (@KoYungRou)
* Anuj Rajendraprasad Nene (@Neneanuj)

---

## **📞 Contact**
For questions, reach out via Big Data Course or open an issue on GitHub.