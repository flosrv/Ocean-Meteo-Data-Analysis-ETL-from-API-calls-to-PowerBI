import retry_requests, re
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from retry_requests import retry
import requests, pandas as pd
from datetime import datetime, timedelta, timezone, time
import os, sys, subprocess, random, warnings
import folium, requests_cache, openmeteo_requests
from ndbc_api import NdbcApi
api = NdbcApi()
from pathlib import Path
from requests.exceptions import HTTPError  # Importer HTTPError
from urllib.error import HTTPError
from siphon.simplewebservice.ndbc import NDBC
from urllib.parse import quote_plus, unquote
import xml.etree.ElementTree as ET
from typing import Optional, List
from pandas import json_normalize
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from IPython.core.display import *
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import UniqueConstraint 
from sqlalchemy.exc import IntegrityError
import json
from sqlalchemy import ForeignKeyConstraint, Float, DateTime, ForeignKey, MetaData, text
from sqlalchemy import create_engine, inspect, Table, select, Column, Integer, String, Time
import logging, ast, glob
from dask import dataframe as dd
from matplotlib import pyplot as plt
import seaborn as sns
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split, RandomizedSearchCV, cross_val_score
from sklearn.ensemble import RandomForestRegressor
from functools import reduce
from operator import mul
from itertools import product


