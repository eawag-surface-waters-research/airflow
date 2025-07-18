import os
import json
import pytz
import requests
import tempfile
import numpy as np
from datetime import datetime, timedelta, timezone
import pandas as pd
from html.parser import HTMLParser
import xml.etree.ElementTree as ET

def ch1903_plus_to_latlng(x, y):
    x_aux = (x - 2600000) / 1000000
    y_aux = (y - 1200000) / 1000000
    lat = 16.9023892 + 3.238272 * y_aux - 0.270978 * x_aux ** 2 - 0.002528 * y_aux ** 2 - 0.0447 * x_aux ** 2 * y_aux - 0.014 * y_aux ** 3
    lng = 2.6779094 + 4.728982 * x_aux + 0.791484 * x_aux * y_aux + 0.1306 * x_aux * y_aux ** 2 - 0.0436 * x_aux ** 3
    lat = (lat * 100) / 36
    lng = (lng * 100) / 36
    return lat, lng
