from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Union
from sqlalchemy import text
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

# 導入已有的 SQL 和 ChatBot 代理
from sql_agent import SQLAgent
from chatbot_agent import ChatbotAgent

app = FastAPI(title="SentimentSift Restaurant Analysis API")

# 初始化 SQL 代理
sql_agent = SQLAgent("snowflake_config.json")

# 初始化聊天機器人代理
chatbot_agent = ChatbotAgent()

# 定義請求模型
class QueryRequest(BaseModel):
    question: str

class ChartDataRequest(BaseModel):
    chart_type: str
    city: Optional[str] = None
    limit: Optional[int] = 50
    min_score: Optional[float] = None
    category: Optional[str] = None

@app.get("/")
async def root():
    return {"message": "Welcome to the SentimentSift API. Visit /docs for API documentation."}

@app.post("/query")
async def natural_language_query(request: QueryRequest):
    """
    將自然語言問題轉換為 SQL 查詢並返回結果
    """
    try:
        # 使用 SQL 代理執行自然語言查詢
        query_result = sql_agent.natural_language_query(request.question)
        
        # 確保結果包含經緯度資訊用於地圖
        if query_result["results"] and len(query_result["results"]) > 0:
            for restaurant in query_result["results"]:
                # 如果資料庫中沒有經緯度，可以使用一個模擬的方法生成
                if "latitude" not in restaurant or "longitude" not in restaurant:
                    # 這裡只是演示，實際應用中應該從資料庫獲取真實資料
                    if "city" in restaurant:
                        # 根據城市生成模擬經緯度（實際應用中應從資料庫獲取）
                        if restaurant["city"] == "Boston":
                            base_lat, base_lng = 42.3601, -71.0589
                        elif restaurant["city"] == "New York":
                            base_lat, base_lng = 40.7128, -74.0060
                        else:
                            base_lat, base_lng = 37.7749, -122.4194
                        
                        # 添加一些小的隨機偏移以區分餐廳位置
                        restaurant["latitude"] = base_lat + (np.random.random() - 0.5) * 0.05
                        restaurant["longitude"] = base_lng + (np.random.random() - 0.5) * 0.05
        
        return query_result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查詢處理錯誤: {str(e)}")

@app.get("/search")
async def search_restaurants(
    q: str,
    city: Optional[str] = None,
    min_score: Optional[float] = None,
    limit: int = 10,
    include_reviews: bool = False
):
    """
    基於參數搜索餐廳
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            r.business_id, 
            r.name, 
            r.address, 
            r.city, 
            r.stars, 
            r.overall_score,
            r.food_score, 
            r.service_score, 
            r.ambiance_score,
            r.latitude,
            r.longitude,
            r.price_range,
            r.categories
        FROM 
            restaurants r
        WHERE 
            1=1
        """
        
        # 添加過濾條件
        params = {}
        if city:
            query += " AND r.city = :city"
            params["city"] = city
        
        if min_score:
            query += " AND r.stars >= :min_score"
            params["min_score"] = min_score
        
        # 添加關鍵字搜索
        if q and q.strip():
            query += """ 
            AND (
                r.name ILIKE :search_term 
                OR r.categories ILIKE :search_term
                OR EXISTS (
                    SELECT 1 FROM reviews v 
                    WHERE v.business_id = r.business_id 
                    AND v.text ILIKE :search_term
                )
            )
            """
            params["search_term"] = f"%{q}%"
        
        # 添加排序和限制
        query += " ORDER BY r.overall_score DESC LIMIT :limit"
        params["limit"] = limit
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        # 如果需要包含評論
        if include_reviews:
            for restaurant in results:
                # 獲取餐廳的評論
                review_query = """
                SELECT 
                    r.review_id, 
                    r.user_id, 
                    r.user_name,
                    r.stars, 
                    r.text, 
                    r.date
                FROM 
                    reviews r
                WHERE 
                    r.business_id = :business_id
                ORDER BY 
                    r.date DESC
                LIMIT 5
                """
                
                review_params = {"business_id": restaurant["business_id"]}
                reviews, _ = sql_agent.execute_query(review_query, review_params)
                restaurant["reviews"] = reviews
        
        # 生成模擬經緯度（如果資料庫中沒有）
        for restaurant in results:
            if "latitude" not in restaurant or "longitude" not in restaurant:
                # 根據城市生成模擬經緯度
                if "city" in restaurant:
                    if restaurant["city"] == "Boston":
                        base_lat, base_lng = 42.3601, -71.0589
                    elif restaurant["city"] == "New York":
                        base_lat, base_lng = 40.7128, -74.0060
                    else:
                        base_lat, base_lng = 37.7749, -122.4194
                    
                    restaurant["latitude"] = base_lat + (np.random.random() - 0.5) * 0.05
                    restaurant["longitude"] = base_lng + (np.random.random() - 0.5) * 0.05
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"搜索錯誤: {str(e)}")

@app.get("/restaurants")
async def get_top_restaurants(
    limit: int = 10,
    city: Optional[str] = None,
    min_score: Optional[float] = None,
    category: Optional[str] = None
):
    """
    獲取頂級餐廳列表
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            r.business_id, 
            r.name, 
            r.address, 
            r.city, 
            r.stars, 
            r.overall_score,
            r.food_score, 
            r.service_score, 
            r.ambiance_score,
            r.latitude,
            r.longitude,
            r.price_range,
            r.categories
        FROM 
            restaurants r
        WHERE 
            1=1
        """
        
        # 添加過濾條件
        params = {}
        if city:
            query += " AND r.city = :city"
            params["city"] = city
        
        if min_score:
            query += " AND r.stars >= :min_score"
            params["min_score"] = min_score
        
        if category:
            query += " AND r.categories ILIKE :category"
            params["category"] = f"%{category}%"
        
        # 添加排序和限制
        query += " ORDER BY r.overall_score DESC LIMIT :limit"
        params["limit"] = limit
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        # 生成模擬經緯度（如果資料庫中沒有）
        for restaurant in results:
            if "latitude" not in restaurant or "longitude" not in restaurant:
                # 根據城市生成模擬經緯度
                if "city" in restaurant:
                    if restaurant["city"] == "Boston":
                        base_lat, base_lng = 42.3601, -71.0589
                    elif restaurant["city"] == "New York":
                        base_lat, base_lng = 40.7128, -74.0060
                    else:
                        base_lat, base_lng = 37.7749, -122.4194
                    
                    restaurant["latitude"] = base_lat + (np.random.random() - 0.5) * 0.05
                    restaurant["longitude"] = base_lng + (np.random.random() - 0.5) * 0.05
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取餐廳錯誤: {str(e)}")

@app.get("/stats/overall")
async def get_overall_stats(city: Optional[str] = None):
    """
    獲取整體統計資料，用於儀表板
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            COUNT(*) as total_restaurants,
            AVG(stars) as avg_rating,
            AVG(food_score) as avg_food_score,
            AVG(service_score) as avg_service_score,
            AVG(ambiance_score) as avg_ambiance_score
        FROM 
            restaurants
        """
        
        params = {}
        if city:
            query += " WHERE city = :city"
            params["city"] = city
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        if not results or len(results) == 0:
            return {
                "total_restaurants": 0,
                "avg_rating": 0,
                "avg_food_score": 0,
                "avg_service_score": 0,
                "avg_ambiance_score": 0
            }
        
        # 城市分布
        city_query = """
        SELECT 
            city, 
            COUNT(*) as count
        FROM 
            restaurants
        GROUP BY 
            city
        ORDER BY 
            count DESC
        LIMIT 10
        """
        
        city_results, _ = sql_agent.execute_query(city_query, {})
        
        # 分類分布
        category_query = """
        SELECT 
            TRIM(c.category) as category, 
            COUNT(*) as count
        FROM 
            restaurants r,
            LATERAL SPLIT_TO_TABLE(r.categories, ',') c(category)
        GROUP BY 
            TRIM(c.category)
        ORDER BY 
            count DESC
        LIMIT 10
        """
        
        category_results, _ = sql_agent.execute_query(category_query, {})
        
        # 組合結果
        return {
            "summary": results[0],
            "cities": city_results,
            "categories": category_results
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取統計錯誤: {str(e)}")

@app.get("/stats/ratings")
async def get_rating_distribution(city: Optional[str] = None):
    """
    獲取評分分布，用於直方圖
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            FLOOR(stars * 2) / 2 as rating_bin,
            COUNT(*) as count
        FROM 
            restaurants
        """
        
        params = {}
        if city:
            query += " WHERE city = :city"
            params["city"] = city
        
        query += """
        GROUP BY 
            rating_bin
        ORDER BY 
            rating_bin
        """
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取評分分布錯誤: {str(e)}")

@app.get("/stats/scores")
async def get_score_comparison(city: Optional[str] = None):
    """
    獲取不同評分類別的比較資料，用於比較圖表
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            AVG(food_score) as avg_food_score,
            AVG(service_score) as avg_service_score,
            AVG(ambiance_score) as avg_ambiance_score,
            STDDEV(food_score) as std_food_score,
            STDDEV(service_score) as std_service_score,
            STDDEV(ambiance_score) as std_ambiance_score,
            MIN(food_score) as min_food_score,
            MIN(service_score) as min_service_score,
            MIN(ambiance_score) as min_ambiance_score,
            MAX(food_score) as max_food_score,
            MAX(service_score) as max_service_score,
            MAX(ambiance_score) as max_ambiance_score
        FROM 
            restaurants
        """
        
        params = {}
        if city:
            query += " WHERE city = :city"
            params["city"] = city
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        if not results or len(results) == 0:
            return {
                "avg_scores": {
                    "Food": 0,
                    "Service": 0,
                    "Ambiance": 0
                },
                "std_scores": {
                    "Food": 0,
                    "Service": 0,
                    "Ambiance": 0
                },
                "min_scores": {
                    "Food": 0,
                    "Service": 0,
                    "Ambiance": 0
                },
                "max_scores": {
                    "Food": 0,
                    "Service": 0,
                    "Ambiance": 0
                }
            }
        
        # 格式化結果
        result = results[0]
        return {
            "avg_scores": {
                "Food": result["avg_food_score"],
                "Service": result["avg_service_score"],
                "Ambiance": result["avg_ambiance_score"]
            },
            "std_scores": {
                "Food": result["std_food_score"],
                "Service": result["std_service_score"],
                "Ambiance": result["std_ambiance_score"]
            },
            "min_scores": {
                "Food": result["min_food_score"],
                "Service": result["min_service_score"],
                "Ambiance": result["min_ambiance_score"]
            },
            "max_scores": {
                "Food": result["max_food_score"],
                "Service": result["max_service_score"],
                "Ambiance": result["max_ambiance_score"]
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取評分比較錯誤: {str(e)}")

@app.get("/stats/top_by_category")
async def get_top_by_category(
    category: str,
    city: Optional[str] = None,
    limit: int = 10
):
    """
    獲取特定類別的頂級餐廳
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            r.business_id, 
            r.name, 
            r.address, 
            r.city, 
            r.stars, 
            r.overall_score,
            r.food_score, 
            r.service_score, 
            r.ambiance_score
        FROM 
            restaurants r
        WHERE 
            r.categories ILIKE :category
        """
        
        params = {"category": f"%{category}%"}
        
        if city:
            query += " AND r.city = :city"
            params["city"] = city
        
        # 依評分排序
        if category.lower() in ["food", "restaurant", "cafe", "bakery"]:
            query += " ORDER BY r.food_score DESC"
        elif category.lower() in ["service", "hospitality"]:
            query += " ORDER BY r.service_score DESC"
        elif category.lower() in ["ambiance", "atmosphere", "environment"]:
            query += " ORDER BY r.ambiance_score DESC"
        else:
            query += " ORDER BY r.overall_score DESC"
        
        query += " LIMIT :limit"
        params["limit"] = limit
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, params)
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取頂級類別錯誤: {str(e)}")

@app.get("/stats/city_comparison")
async def get_city_comparison():
    """
    獲取不同城市的餐廳評分比較
    """
    try:
        # 構建 SQL 查詢
        query = """
        SELECT 
            city,
            COUNT(*) as restaurant_count,
            AVG(stars) as avg_rating,
            AVG(food_score) as avg_food_score,
            AVG(service_score) as avg_service_score,
            AVG(ambiance_score) as avg_ambiance_score
        FROM 
            restaurants
        GROUP BY 
            city
        ORDER BY 
            restaurant_count DESC
        LIMIT 10
        """
        
        # 執行查詢
        results, _ = sql_agent.execute_query(query, {})
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"獲取城市比較錯誤: {str(e)}")

# 運行服務器
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)