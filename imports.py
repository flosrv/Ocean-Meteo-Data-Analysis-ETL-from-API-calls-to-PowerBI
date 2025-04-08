import retry_requests, re
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
import time
import tkinter as tk
from urllib.parse import quote_plus, unquote
import xml.etree.ElementTree as ET
from sqlmodel import SQLModel, Field, create_engine, Relationship, Session
from typing import Optional, List
from pandas import json_normalize
import numpy as np
from folium import CircleMarker
import geopandas as gpd
from shapely.geometry import Point
from IPython.core.display import *
import json
from sqlalchemy import create_engine, Integer, String, Float, DateTime, Column, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import UniqueConstraint  # Importation de UniqueConstraint