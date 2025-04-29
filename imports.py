import os, sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import re, subprocess, random, warnings, time, json, logging, ast, glob
import requests, pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from functools import reduce
from operator import mul
from itertools import product
from urllib.parse import quote_plus, unquote, urljoin
from urllib.error import HTTPError
from requests.exceptions import HTTPError as RequestsHTTPError
from IPython.display import display, HTML

import retry_requests
from retry_requests import retry
import requests_cache

import xml.etree.ElementTree as ET
from fastapi import FastAPI, Query
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely.geometry import Point

import openmeteo_requests
from ndbc_api import NdbcApi
api = NdbcApi()

from siphon.simplewebservice.ndbc import NDBC

from sqlalchemy import create_engine, inspect, MetaData, Table, select, Boolean,func
from sqlalchemy import Column, Integer, String, Time, Float, DateTime, ForeignKey, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy import ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Session
from enum import Enum

from sqlalchemy.orm import Session, sessionmaker
from contextlib import asynccontextmanager
from sqlmodel import SQLModel, Field
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split, RandomizedSearchCV, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from xgboost import XGBRegressor

from matplotlib import pyplot as plt
import seaborn as sns

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC

from typing import List, Optional,Union,Dict, Any, Annotated
from sqlalchemy import select, func, MetaData, asc, and_
from pydantic import BaseModel, Field
