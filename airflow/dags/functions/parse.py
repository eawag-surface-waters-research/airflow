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


class TableHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.current_row = []
        self.in_td = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.in_td = True

    def handle_endtag(self, tag):
        if tag == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)
                self.current_row = []
        elif tag == 'td':
            self.in_td = False

    def handle_data(self, data):
        if self.in_td:
            self.current_row.append(data.strip())


def parse_html_table(html_table):
    parser = TableHTMLParser()
    parser.feed(html_table)
    df = pd.DataFrame(parser.rows)
    return df

class CustomHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.root = ET.Element("root")
        self.current = self.root
        self.stack = [self.root]  # Stack to keep track of parent elements

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        element = ET.SubElement(self.current, tag, attrs_dict)
        self.stack.append(self.current)  # Push current element to stack
        self.current = element

    def handle_endtag(self, tag):
        self.current = self.stack.pop()  # Pop from stack to move to the parent

    def handle_data(self, data):
        if self.current.text is None:
            self.current.text = data
        else:
            self.current.text += data

def parse_html(html_string):
    parser = CustomHTMLParser()
    parser.feed(html_string)
    return parser.root

def html_find_all(element, tag=None, class_name=None, attributes=None):
    def match_attributes(el, attributes):
        if not attributes:
            return True
        for attr, value in attributes.items():
            if el.get(attr) != value:
                return False
        return True

    def match_class_name(el, class_name):
        if class_name is None:
            return True
        return class_name in el.get('class', '').split()

    # Recursively search for matching elements
    def find_all_recursive(el, matches):
        if ((tag is None or el.tag == tag) and
            match_class_name(el, class_name) and
            match_attributes(el, attributes)):
            matches.append(el)
        for child in el:
            find_all_recursive(child, matches)

    matches = []
    find_all_recursive(element, matches)
    return matches