# 네이버 검색 API예제는 블로그를 비롯 전문자료까지 호출방법이 동일하므로 blog검색만 대표로 예제를 올렸습니다.
# 네이버 검색 Open API 예제 - 블로그 검색
import os
import sys
import urllib.request
import json
import csv
import pandas as pd
import time
from pandas import DataFrame

from pathlib import Path
from com_dayoung_api.utils.file_helper import FileReader
