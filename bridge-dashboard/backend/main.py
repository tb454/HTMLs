import asyncio
import logging
import random
import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional

import jwt
import joblib
import requests
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Request,
    status,
    WebSocket,
    WebSocketDisconnect
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlalchemy import Column, Integer, String, DateTime, Float, Index, create_engine
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime 
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from passlib.context import CryptContext

Base = declarative_base()

# --------------------------
# Configuration Using Pydantic Settings
# --------------------------
class Settings(BaseSettings):
    database_url: str = "sqlite:///./trading_bridge.db"
    secret_key: str = "your-secret-key"  # Replace with a strong key
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    rate_limit_requests: int = 5  # per minute for demonstration
    celery_broker_url: str = "redis://localhost:6379/0"  # Example broker URL
    email_alert_recipient: str = "admin@example.com"

    class Config:
        env_file = ".env"

settings = Settings()

# --------------------------
# Enhanced Logging Setup (Rotating File Handler)
# --------------------------
from logging.handlers import RotatingFileHandler

handler = RotatingFileHandler("trading_bridge.log", maxBytes=10*1024*1024, backupCount=3)
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

# --------------------------
# Celery Setup (For Background Task Scheduling)
# --------------------------
from celery import Celery

celery_app = Celery(
    "tasks",
    broker=settings.celery_broker_url,
    backend="rpc://"
)

@celery_app.task
def celery_update_futures_prices():
    # This task will run in a Celery worker process.
    db = SessionLocal()
    try:
        futures_items = db.query(Futures).all()
        for item in futures_items:
            fluctuation = random.uniform(-5, 5)
            item.price = round(item.price + fluctuation, 2)
            item.timestamp = datetime.utcnow()
        db.commit()
        logger.info("Celery: Futures prices updated.")
        # Broadcast update via WebSocket (call from main event loop)
        asyncio.run(manager.broadcast({
            "type": "futures_update",
            "data": [
                {"symbol": f.symbol, "price": f.price, "timestamp": f.timestamp.isoformat()}
                for f in futures_items
            ]
        }))
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Celery: Error updating futures prices: {e}")
    finally:
        db.close()

# --------------------------
# Rate Limiting Dependency (Simple In-Memory Limiter)
# --------------------------
import time

RATE_LIMIT = settings.rate_limit_requests
# A very simple dictionary to track IP addresses and request timestamps.
rate_limit_cache = {}

def rate_limiter(request: Request):
    client_ip = request.client.host
    current_time = time.time()
    window = 60  # 60 seconds window
    request_times = rate_limit_cache.get(client_ip, [])
    # Remove outdated timestamps.
    request_times = [t for t in request_times if current_time - t < window]
    if len(request_times) >= RATE_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Too many requests, please try again later."
        )
    request_times.append(current_time)
    rate_limit_cache[client_ip] = request_times

# --------------------------
# SQLAlchemy Setup
# --------------------------
engine = create_engine(settings.database_url, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --------------------------
# Password Hashing and Auth Setup
# --------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --------------------------
# Database Models
# --------------------------
class ScrapMetal(Base):
    __tablename__ = "scrap_metals"
    id = Column(Integer, primary_key=True, index=True)
    metal_type = Column(String, index=True)
    quantity = Column(Float)
    quality = Column(String)
    location = Column(String)
    price = Column(Float)

class Futures(Base):
    __tablename__ = "futures"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    metal_type = Column(String)
    order_type = Column(String)  # "buy" or "sell"
    quantity = Column(Float)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)

# Create all tables
Base.metadata.create_all(bind=engine)

# --------------------------
# Pydantic Schemas
# --------------------------
class ScrapMetal(Base):
    __tablename__ = "scrap_metal"

    id = Column(Integer, primary_key=True, index=True)
    metal_type = Column(String, index=True)
    quantity = Column(Float)
    quality = Column(String)
    location = Column(String, index=True)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

# Composite index for filtering by type and location
Index("idx_scrap_type_location", ScrapMetal.metal_type, ScrapMetal.location)

class ScrapMetalCreate(BaseModel):
    metal_type: str
    quantity: float
    quality: str
    location: str
    price: float

class Futures(Base):
    __tablename__ = "futures"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    expiry_date = Column(DateTime, index=True)
    contract_price = Column(Float)
    created_at = Column(DateTime)

Index("idx_symbol_expiry", Futures.symbol, Futures.expiry_date)

# ✅ Composite index — placed OUTSIDE the class
Index("idx_symbol_expiry", Futures.symbol, Futures.expiry_date)

class FuturesUpdate(BaseModel):
    symbol: str
    price: float

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    metal_type = Column(String, index=True)
    order_type = Column(String, index=True)  # e.g., "buy" or "sell"
    quantity = Column(Float)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

# Composite index for querying orders by metal type and order type
Index("idx_order_type_metal", Order.metal_type, Order.order_type)

class OrderCreate(BaseModel):
    metal_type: str
    order_type: str
    quantity: float
    price: float

class RiskReport(BaseModel):
    total_scrap_value: float
    total_order_value: float
    risk_exposure: float
    timestamp: datetime

# User Schemas
class UserCreate(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

# --------------------------
# Utility Functions
# --------------------------
def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
    return encoded_jwt

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exception
    return user

# --------------------------
# WebSocket Connection Manager
# --------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("WebSocket client connected.")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info("WebSocket client disconnected.")

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")

manager = ConnectionManager()

# --------------------------
# External API Integration Example
# --------------------------
def fetch_real_futures_data(symbol: str) -> dict:
    # Replace with actual API endpoint and logic as needed
    url = f"https://api.example.com/markets/{symbol}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch external futures data")
    return response.json()

# --------------------------
# Machine Learning Prediction Example
# --------------------------
try:
    ml_model = joblib.load("price_prediction_model.pkl")
    logger.info("ML model loaded successfully.")
except Exception as e:
    ml_model = None
    logger.error(f"Error loading ML model: {e}")

# --------------------------
# Email Notification Stub
# --------------------------
def send_email_alert(subject: str, message: str, recipient: str = settings.email_alert_recipient):
    # In a production system, integrate with an email service (e.g., SMTP, SendGrid, etc.)
    logger.info(f"Sending email alert to {recipient}: Subject: {subject}, Message: {message}")

# --------------------------
# Background Task: Update Futures Prices & Broadcast
# --------------------------
def update_futures_prices(db: Session):
    futures_items = db.query(Futures).all()
    for item in futures_items:
        fluctuation = random.uniform(-5, 5)
        item.price = round(item.price + fluctuation, 2)
        item.timestamp = datetime.utcnow()
    try:
        db.commit()
        logger.info("Futures prices updated in DB.")
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error updating futures prices: {e}")
    # Broadcast updated prices
    asyncio.run(manager.broadcast({
        "type": "futures_update",
        "data": [
            {"symbol": f.symbol, "price": f.price, "timestamp": f.timestamp.isoformat()}
            for f in futures_items
        ]
    }))

def background_futures_updater():
    while True:
        db = SessionLocal()
        update_futures_prices(db)
        db.close()
        time.sleep(30)  # Update every 30 seconds

# Start a background thread for futures updates (as a fallback to Celery)
threading.Thread(target=background_futures_updater, daemon=True).start()

# --------------------------
# FastAPI Application Setup
# --------------------------
app = FastAPI(title="Scrap Metal & Futures Trading Bridge API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging Middleware with Rate Limiting
@app.middleware("http")
async def log_requests(request: Request, call_next):
    rate_limiter(request)  # enforce rate limit
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Completed response: {response.status_code}")
    return response

# --------------------------
# API Endpoints
# --------------------------
@app.get("/")
def read_root():
    return {"message": "Welcome to the Trading Bridge API"}

# User Registration and Authentication
@app.post("/users/register", response_model=Token)
def register(user_create: UserCreate, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == user_create.username).first():
        raise HTTPException(status_code=400, detail="Username already registered")
    hashed_password = get_password_hash(user_create.password)
    new_user = User(username=user_create.username, hashed_password=hashed_password)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    access_token = create_access_token(data={"sub": new_user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/token", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

# Scrap Metal Endpoints
@app.get("/scrap-metal", response_model=List[ScrapMetalItem])
def read_scrap_metals(db: Session = Depends(get_db)):
    return db.query(ScrapMetal).all()

@app.get("/scrap-metal/{item_id}", response_model=ScrapMetalItem)
def read_scrap_metal(item_id: int, db: Session = Depends(get_db)):
    item = db.query(ScrapMetal).filter(ScrapMetal.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Scrap metal item not found")
    return item

@app.post("/scrap-metal", response_model=ScrapMetalItem, status_code=201)
def create_scrap_metal(item: ScrapMetalCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    new_item = ScrapMetal(**item.dict())
    db.add(new_item)
    db.commit()
    db.refresh(new_item)
    return new_item

# Futures Data Endpoints
@app.get("/futures", response_model=List[FuturesData])
def read_futures(db: Session = Depends(get_db)):
    return db.query(Futures).all()

@app.post("/futures", response_model=FuturesData, status_code=201)
def update_or_create_futures(update: FuturesUpdate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    symbol_upper = update.symbol.upper()
    futures_item = db.query(Futures).filter(Futures.symbol == symbol_upper).first()
    if futures_item:
        futures_item.price = update.price
        futures_item.timestamp = datetime.utcnow()
    else:
        futures_item = Futures(symbol=symbol_upper, price=update.price, timestamp=datetime.utcnow())
        db.add(futures_item)
    db.commit()
    db.refresh(futures_item)
    return futures_item

# External API Integration Endpoint
@app.get("/external/futures/{symbol}")
def get_external_futures(symbol: str):
    try:
        data = fetch_real_futures_data(symbol)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Order Endpoints
@app.get("/orders", response_model=List[OrderItem])
def read_orders(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    return db.query(Order).all()

@app.post("/orders", response_model=OrderItem, status_code=201)
def create_order(order: OrderCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    new_order = Order(**order.dict(), timestamp=datetime.utcnow())
    db.add(new_order)
    db.commit()
    db.refresh(new_order)
    return new_order

# Order Matching Endpoint (Enhanced Order Management)
def match_orders(db: Session):
    buy_orders = db.query(Order).filter(Order.order_type == "buy").all()
    sell_orders = db.query(Order).filter(Order.order_type == "sell").all()
    matches = []
    for buy in buy_orders:
        for sell in sell_orders:
            if buy.metal_type == sell.metal_type and buy.price >= sell.price:
                match_details = {
                    "metal_type": buy.metal_type,
                    "buy_order_id": buy.id,
                    "sell_order_id": sell.id,
                    "execution_price": round((buy.price + sell.price) / 2, 2),
                    "quantity": min(buy.quantity, sell.quantity)
                }
                matches.append(match_details)
                # Update or remove orders as appropriate in a real system
    return matches

@app.get("/order-matches")
def get_order_matches(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    return match_orders(db)

# Risk Management Reporting Endpoint
@app.get("/risk-report", response_model=RiskReport)
def risk_report(db: Session = Depends(get_db)):
    scrap_metals = db.query(ScrapMetal).all()
    orders = db.query(Order).all()
    total_scrap_value = sum(item.price * item.quantity for item in scrap_metals)
    total_order_value = sum(order.price * order.quantity for order in orders)
    risk_exposure = round(total_order_value - total_scrap_value, 2)
    # If risk exceeds a threshold, send an email alert (threshold value is arbitrary here)
    if risk_exposure > 10000:
        send_email_alert("Risk Alert", f"Risk exposure has exceeded threshold: {risk_exposure}")
    return RiskReport(
        total_scrap_value=total_scrap_value,
        total_order_value=total_order_value,
        risk_exposure=risk_exposure,
        timestamp=datetime.utcnow()
    )

# Analytics Endpoint: Compare Scrap vs. Futures Prices
@app.get("/analytics")
def analytics(db: Session = Depends(get_db)):
    scrap_metals = db.query(ScrapMetal).all()
    futures_items = db.query(Futures).all()
    futures_map = {f.symbol: f for f in futures_items}
    result = []
    for scrap in scrap_metals:
        metal_key = scrap.metal_type.upper()
        if metal_key in futures_map:
            future = futures_map[metal_key]
            price_diff = round(future.price - scrap.price, 2)
            result.append({
                "metal_type": scrap.metal_type,
                "scrap_price": scrap.price,
                "futures_price": future.price,
                "price_difference": price_diff
            })
        else:
            result.append({
                "metal_type": scrap.metal_type,
                "message": "No futures data available for this metal"
            })
    return result

# Machine Learning Prediction Endpoint (Advanced Analytics)
@app.post("/predict-price")
def predict_price(item: ScrapMetalCreate):
    if ml_model is None:
        raise HTTPException(status_code=500, detail="ML model not available")
    import numpy as np
    features = np.array([[item.quantity, item.price]])
    predicted_price = ml_model.predict(features)
    return {"predicted_price": float(predicted_price[0])}

# WebSocket Endpoint for Real-Time Futures Updates
@app.websocket("/ws/futures")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    await manager.connect(websocket)
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        user_id = payload.get("sub")
        # Optional: load user from DB and check if still active
        await websocket.accept()
    except Exception:
        await websocket.close(code=1008)  # Policy Violation
        return 
    try: 
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Endpoint to trigger manual futures update using background tasks
@app.post("/update-futures")
def trigger_futures_update(background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    # Schedule a Celery task instead of a direct update
    celery_update_futures_prices.delay()
    background_tasks.add_task(update_futures_prices, db)
    return {"message": "Futures data update triggered"}

# Admin Monitoring Endpoint (for system metrics, etc.)
@app.get("/admin/health")
def admin_health(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    # A simple health check endpoint
    scrap_count = db.query(ScrapMetal).count()
    orders_count = db.query(Order).count()
    futures_count = db.query(Futures).count()
    return {
        "status": "ok",
        "scrap_metals": scrap_count,
        "orders": orders_count,
        "futures": futures_count,
        "timestamp": datetime.utcnow().isoformat()
    }

# Global Error Handler Example for HTTPExceptions
@app.exception_handler(HTTPException)
def custom_http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTPException: {exc.detail}")
    return {"error": exc.detail}
import requests

def fetch_alpha_vantage_data(symbol: str) -> dict:
    # Ensure you've set your API key in your settings or .env file
    api_key = settings.alpha_vantage_api_key  # e.g., "YOUR_ALPHA_VANTAGE_API_KEY"
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",  # Adjust function based on your needs
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch data from Alpha Vantage")
    return response.json()

@app.get("/external/futures/alphavantage/{symbol}")
def get_alpha_vantage_futures(symbol: str):
    try:
        data = fetch_alpha_vantage_data(symbol)
        # Optionally transform data here to match your schema
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_weekly_data(symbol: str) -> dict:
    api_key = settings.alpha_vantage_api_key  # Your API key from the .env file
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_WEEKLY",
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch weekly data from Alpha Vantage")
    return response.json()
@app.get("/external/futures/alphavantage/weekly/{symbol}")
def get_weekly_data(symbol: str):
    try:
        data = fetch_weekly_data(symbol)
        # Optionally transform the data to match your needs
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_monthly_data(symbol: str) -> dict:
    api_key = settings.alpha_vantage_api_key  # Your API key from your .env file
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_MONTHLY",
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch monthly data from Alpha Vantage")
    return response.json()
@app.get("/external/futures/alphavantage/monthly/{symbol}")
def get_monthly_data(symbol: str):
    try:
        data = fetch_monthly_data(symbol)
        # Optionally, transform the data to match your internal schema if needed
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_historical_options_data(symbol: str) -> dict:
    api_key = settings.alpha_vantage_api_key  # This pulls your API key from .env via your Settings class
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "HISTORICAL_OPTIONS",  # The function for historical options data
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch historical options data from Alpha Vantage")
    return response.json()
@app.get("/external/options/historical/{symbol}")
def get_historical_options(symbol: str):
    try:
        data = fetch_historical_options_data(symbol)
        # Optionally, transform the data to match your schema
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_news_sentiment_data(tickers: str) -> dict:
    api_key = settings.alpha_vantage_api_key  # Your API key from .env via Settings
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": tickers,  # Pass a comma-separated list if needed
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch news sentiment data from Alpha Vantage")
    return response.json()
@app.get("/external/news-sentiment/{tickers}")
def get_news_sentiment(tickers: str):
    try:
        data = fetch_news_sentiment_data(tickers)
        # Optionally, transform or filter the data here if needed
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_advanced_analytics(symbols: str, start_date: str, end_date: str, 
                             interval: str = "DAILY", ohlc: str = "close", 
                             calculations: str = "MEAN,STDDEV,CORRELATION") -> dict:
    api_key = settings.alpha_vantage_api_key  # Your API key from .env via Settings
    base_url = "https://alphavantageapi.co/timeseries/analytics"
    params = {
        "SYMBOLS": symbols,  # e.g., "AAPL,MSFT,IBM"
        "RANGE": [start_date, end_date],  # Pass both dates as a list
        "INTERVAL": interval,
        "OHLC": ohlc,
        "CALCULATIONS": calculations,
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch advanced analytics data from Alpha Vantage")
    return response.json()
@app.get("/external/analytics/advanced")
def get_advanced_analytics(symbols: str, start_date: str, end_date: str, 
                           interval: str = "DAILY", ohlc: str = "close", 
                           calculations: str = "MEAN,STDDEV,CORRELATION"):
    try:
        data = fetch_advanced_analytics(symbols, start_date, end_date, interval, ohlc, calculations)
        # Optionally, transform data here to match your internal schema
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_global_copper_price() -> dict:
    api_key = settings.alpha_vantage_api_key  # Your API key from .env via Settings
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "COPPER",      # Function to get global copper price
        "interval": "monthly",     # Using monthly interval
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch global copper price from Alpha Vantage")
    return response.json()
@app.get("/external/copper/global")
def get_global_copper_price():
    try:
        data = fetch_global_copper_price()
        # Optionally, transform the data to fit your schema if needed.
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_global_aluminum_price() -> dict:
    api_key = settings.alpha_vantage_api_key  # This value is loaded from your .env via Settings
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "ALUMINUM",   # Requesting global aluminum data
        "interval": "monthly",    # Monthly interval data
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch global aluminum price from Alpha Vantage")
    return response.json()
@app.get("/external/aluminum/global")
def get_global_aluminum_price():
    try:
        data = fetch_global_aluminum_price()
        # Optionally, transform data here to fit your internal schema if needed.
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def fetch_all_commodities_data() -> dict:
    api_key = settings.alpha_vantage_api_key  # Loaded from your .env via your Settings class
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "ALL_COMMODITIES",  # Request all commodities data
        "interval": "monthly",          # Monthly data aggregation
        "apikey": api_key
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception("Failed to fetch global commodities data from Alpha Vantage")
    return response.json()
@app.get("/external/commodities/global")
def get_global_commodities_data():
    try:
        data = fetch_all_commodities_data()
        # Optionally, transform data to match your schema here
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------
# Run the Application
# --------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
