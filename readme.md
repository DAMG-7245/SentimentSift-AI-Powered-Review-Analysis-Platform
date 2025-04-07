# README.md

## **SentimentSift**

### **Introduction**
SentimentSift is a data-driven tool designed to transform unstructured customer feedback into actionable insights for the restaurant industry. By consolidating reviews and ratings from multiple platforms, the tool generates a normalized rating system based on three critical aspects: **Food**, **Service**, and **Ambiance**. 

Additionally, SentimentSift integrates an advanced chatbot powered by **Retrieval-Augmented Generation (RAG)** and **Large Language Models (LLMs)** to provide precise, context-aware responses to user queries. This project aims to empower restaurant owners, customers, and market researchers with unbiased insights and interactive tools for better decision-making.

---

## **Overview**

### **Key Features**
1. **Sentiment Analysis**: Aspect-based sentiment scoring for food, service, and ambiance using PyABSA.
2. **Theme Extraction**: Identification of recurring themes in reviews with BERTopic.
3. **Normalized Ratings**: Consolidated scores visualized on a scale of 10/100.
4. **Interactive UI**: A Streamlit-based interface for querying trends and visualizing insights.
5. **Chatbot Integration**:
   - RAG-powered chatbot for advanced restaurant-specific queries.
   - Context-aware responses using LLMs (e.g., OpenAI GPT-4).
6. **Actionable Reports**: Tiered summary reports highlighting trends in customer feedback.

---

### **System Architecture**

![Alt Text](WorkFlow.jpg)

```plaintext
1. Data Ingestion:
   - Collect structured (restaurant details) and unstructured (reviews) data via APIs (Google, Yelp, Twitter).

2. Data Preprocessing:
   - Normalize ratings across platforms; clean text; add metadata.

3. Data Storage:
   - Store raw JSON files in Amazon S3; stage preprocessed data in Snowflake.

4. Sentiment Analysis & Theme Extraction:
   - Use PyABSA for aspect-based scoring; feed labeled text into BERTopic.

5. Vector Embedding Storage:
   - Store embeddings with metadata in Pinecone.

6. Chatbot Integration:
   - Use LangChain & GPT-4 to build a RAG-powered chatbot for advanced queries.

7. Visualization & Querying:
   - Develop Streamlit UI for querying trends and visualizing insights.
```

---

### **Data Sources**

#### **Unstructured Data**
- **Google API**: Reviews and ratings from Google.
- **Yelp API**: Detailed reviews and metadata about restaurants.
- **Twitter API**: Social media analytics for real-time mentions.

#### **Structured Data**
- Restaurant details such as name, location, price range, cuisine type, and operating hours.

---

### **Tools and Technologies**
1. **PyABSA**: For aspect-based sentiment analysis.
2. **BERTopic**: For extracting recurring themes from customer reviews.
3. **Pinecone**: Vector database for storing embeddings with metadata.
4. **Snowflake**: Cloud-based data warehouse for preprocessing and validation.
5. **Streamlit**: Framework for building an interactive user interface.
6. **LangChain & OpenAI GPT-4**: For RAG-powered chatbot integration.
7. **Amazon S3**: Storage for raw JSON files and processed datasets.

---


## **👨‍💻 Authors**
* Sicheng Bao (@Jellysillyfish13)
* Yung Rou Ko (@KoYungRou)
* Anuj Rajendraprasad Nene (@Neneanuj)

---

## **📞 Contact**
For questions, reach out via Big Data Course or open an issue on GitHub.
