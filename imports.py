import retry_requests, re
import sqlalchemy as sa
from retry_requests import retry
import requests, pandas as pd
from datetime import datetime, timedelta, timezone, time
import os, json, re, sys, psycopg2, sys, subprocess, random, warnings
import folium, requests_cache, openmeteo_requests, motor
from ndbc_api import NdbcApi
api = NdbcApi()
from pathlib import Path
from requests.exceptions import HTTPError  # Importer HTTPError
from urllib.error import HTTPError
from siphon.simplewebservice.ndbc import NDBC
from sqlalchemy import create_engine, MetaData, Column,inspect,Interval, text
from sqlalchemy import Table, Column, Integer, String, Boolean, Float, TIMESTAMP, DateTime, Time 
from sqlalchemy.exc import ProgrammingError,OperationalError, NoSuchTableError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
import time
import tkinter as tk
from sqlalchemy.ext.declarative import declarative_base
from urllib.parse import quote_plus, unquote
import xml.etree.ElementTree as ET
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pandas import json_normalize
import sqlalchemy
import numpy as np
from folium import CircleMarker
import geopandas as gpd
from shapely.geometry import Point
from IPython.core.display import *