import os
import re
import torch
from tqdm import tqdm
import hashlib


from config import *
from utils import *

filelist = [
 'ta1-cadets-1-e5-official-2.bin.100.json',
 'ta1-cadets-1-e5-official-2.bin.100.json.1',
 'ta1-cadets-1-e5-official-2.bin.100.json.2',
 'ta1-cadets-1-e5-official-2.bin.101.json',
 'ta1-cadets-1-e5-official-2.bin.101.json.1',
 'ta1-cadets-1-e5-official-2.bin.101.json.2',
 'ta1-cadets-1-e5-official-2.bin.102.json',
 'ta1-cadets-1-e5-official-2.bin.102.json.1',
 'ta1-cadets-1-e5-official-2.bin.102.json.2',
 'ta1-cadets-1-e5-official-2.bin.103.json',
 'ta1-cadets-1-e5-official-2.bin.103.json.1',
 'ta1-cadets-1-e5-official-2.bin.103.json.2',
 'ta1-cadets-1-e5-official-2.bin.104.json',
 'ta1-cadets-1-e5-official-2.bin.104.json.1',
 'ta1-cadets-1-e5-official-2.bin.104.json.2',
 'ta1-cadets-1-e5-official-2.bin.105.json',
 'ta1-cadets-1-e5-official-2.bin.105.json.1',
 'ta1-cadets-1-e5-official-2.bin.105.json.2',
 'ta1-cadets-1-e5-official-2.bin.106.json',
 'ta1-cadets-1-e5-official-2.bin.106.json.1',
 'ta1-cadets-1-e5-official-2.bin.106.json.2',
 'ta1-cadets-1-e5-official-2.bin.107.json',
 'ta1-cadets-1-e5-official-2.bin.107.json.1',
 'ta1-cadets-1-e5-official-2.bin.107.json.2',
 'ta1-cadets-1-e5-official-2.bin.108.json',
 'ta1-cadets-1-e5-official-2.bin.108.json.1',
 'ta1-cadets-1-e5-official-2.bin.108.json.2',
 'ta1-cadets-1-e5-official-2.bin.109.json',
 'ta1-cadets-1-e5-official-2.bin.109.json.1',
 'ta1-cadets-1-e5-official-2.bin.109.json.2',
 'ta1-cadets-1-e5-official-2.bin.10.json',
 'ta1-cadets-1-e5-official-2.bin.10.json.1',
 'ta1-cadets-1-e5-official-2.bin.10.json.2',
 'ta1-cadets-1-e5-official-2.bin.110.json',
 'ta1-cadets-1-e5-official-2.bin.110.json.1',
 'ta1-cadets-1-e5-official-2.bin.110.json.2',
 'ta1-cadets-1-e5-official-2.bin.111.json',
 'ta1-cadets-1-e5-official-2.bin.111.json.1',
 'ta1-cadets-1-e5-official-2.bin.111.json.2',
 'ta1-cadets-1-e5-official-2.bin.112.json',
 'ta1-cadets-1-e5-official-2.bin.112.json.1',
 'ta1-cadets-1-e5-official-2.bin.112.json.2',
 'ta1-cadets-1-e5-official-2.bin.113.json',
 'ta1-cadets-1-e5-official-2.bin.113.json.1',
 'ta1-cadets-1-e5-official-2.bin.113.json.2',
 'ta1-cadets-1-e5-official-2.bin.114.json',
 'ta1-cadets-1-e5-official-2.bin.114.json.1',
 'ta1-cadets-1-e5-official-2.bin.114.json.2',
 'ta1-cadets-1-e5-official-2.bin.115.json',
 'ta1-cadets-1-e5-official-2.bin.115.json.1',
 'ta1-cadets-1-e5-official-2.bin.115.json.2',
 'ta1-cadets-1-e5-official-2.bin.116.json',
 'ta1-cadets-1-e5-official-2.bin.116.json.1',
 'ta1-cadets-1-e5-official-2.bin.116.json.2',
 'ta1-cadets-1-e5-official-2.bin.117.json',
 'ta1-cadets-1-e5-official-2.bin.117.json.1',
 'ta1-cadets-1-e5-official-2.bin.117.json.2',
 'ta1-cadets-1-e5-official-2.bin.118.json',
 'ta1-cadets-1-e5-official-2.bin.118.json.1',
 'ta1-cadets-1-e5-official-2.bin.118.json.2',
 'ta1-cadets-1-e5-official-2.bin.119.json',
 'ta1-cadets-1-e5-official-2.bin.119.json.1',
 'ta1-cadets-1-e5-official-2.bin.119.json.2',
 'ta1-cadets-1-e5-official-2.bin.11.json',
 'ta1-cadets-1-e5-official-2.bin.11.json.1',
 'ta1-cadets-1-e5-official-2.bin.11.json.2',
 'ta1-cadets-1-e5-official-2.bin.120.json',
 'ta1-cadets-1-e5-official-2.bin.120.json.1',
 'ta1-cadets-1-e5-official-2.bin.120.json.2',
 'ta1-cadets-1-e5-official-2.bin.121.json',
 'ta1-cadets-1-e5-official-2.bin.121.json.1',
 'ta1-cadets-1-e5-official-2.bin.12.json',
 'ta1-cadets-1-e5-official-2.bin.12.json.1',
 'ta1-cadets-1-e5-official-2.bin.12.json.2',
 'ta1-cadets-1-e5-official-2.bin.13.json',
 'ta1-cadets-1-e5-official-2.bin.13.json.1',
 'ta1-cadets-1-e5-official-2.bin.13.json.2',
 'ta1-cadets-1-e5-official-2.bin.14.json',
 'ta1-cadets-1-e5-official-2.bin.14.json.1',
 'ta1-cadets-1-e5-official-2.bin.14.json.2',
 'ta1-cadets-1-e5-official-2.bin.15.json',
 'ta1-cadets-1-e5-official-2.bin.15.json.1',
 'ta1-cadets-1-e5-official-2.bin.15.json.2',
 'ta1-cadets-1-e5-official-2.bin.16.json',
 'ta1-cadets-1-e5-official-2.bin.16.json.1',
 'ta1-cadets-1-e5-official-2.bin.16.json.2',
 'ta1-cadets-1-e5-official-2.bin.17.json',
 'ta1-cadets-1-e5-official-2.bin.17.json.1',
 'ta1-cadets-1-e5-official-2.bin.17.json.2',
 'ta1-cadets-1-e5-official-2.bin.18.json',
 'ta1-cadets-1-e5-official-2.bin.18.json.1',
 'ta1-cadets-1-e5-official-2.bin.18.json.2',
 'ta1-cadets-1-e5-official-2.bin.19.json',
 'ta1-cadets-1-e5-official-2.bin.19.json.1',
 'ta1-cadets-1-e5-official-2.bin.19.json.2',
 'ta1-cadets-1-e5-official-2.bin.1.json',
 'ta1-cadets-1-e5-official-2.bin.1.json.1',
 'ta1-cadets-1-e5-official-2.bin.1.json.2',
 'ta1-cadets-1-e5-official-2.bin.20.json',
 'ta1-cadets-1-e5-official-2.bin.20.json.1',
 'ta1-cadets-1-e5-official-2.bin.20.json.2',
 'ta1-cadets-1-e5-official-2.bin.21.json',
 'ta1-cadets-1-e5-official-2.bin.21.json.1',
 'ta1-cadets-1-e5-official-2.bin.21.json.2',
 'ta1-cadets-1-e5-official-2.bin.22.json',
 'ta1-cadets-1-e5-official-2.bin.22.json.1',
 'ta1-cadets-1-e5-official-2.bin.22.json.2',
 'ta1-cadets-1-e5-official-2.bin.23.json',
 'ta1-cadets-1-e5-official-2.bin.23.json.1',
 'ta1-cadets-1-e5-official-2.bin.23.json.2',
 'ta1-cadets-1-e5-official-2.bin.24.json',
 'ta1-cadets-1-e5-official-2.bin.24.json.1',
 'ta1-cadets-1-e5-official-2.bin.24.json.2',
 'ta1-cadets-1-e5-official-2.bin.25.json',
 'ta1-cadets-1-e5-official-2.bin.25.json.1',
 'ta1-cadets-1-e5-official-2.bin.25.json.2',
 'ta1-cadets-1-e5-official-2.bin.26.json',
 'ta1-cadets-1-e5-official-2.bin.26.json.1',
 'ta1-cadets-1-e5-official-2.bin.26.json.2',
 'ta1-cadets-1-e5-official-2.bin.27.json',
 'ta1-cadets-1-e5-official-2.bin.27.json.1',
 'ta1-cadets-1-e5-official-2.bin.27.json.2',
 'ta1-cadets-1-e5-official-2.bin.28.json',
 'ta1-cadets-1-e5-official-2.bin.28.json.1',
 'ta1-cadets-1-e5-official-2.bin.28.json.2',
 'ta1-cadets-1-e5-official-2.bin.29.json',
 'ta1-cadets-1-e5-official-2.bin.29.json.1',
 'ta1-cadets-1-e5-official-2.bin.29.json.2',
 'ta1-cadets-1-e5-official-2.bin.2.json',
 'ta1-cadets-1-e5-official-2.bin.2.json.1',
 'ta1-cadets-1-e5-official-2.bin.2.json.2',
 'ta1-cadets-1-e5-official-2.bin.30.json',
 'ta1-cadets-1-e5-official-2.bin.30.json.1',
 'ta1-cadets-1-e5-official-2.bin.30.json.2',
 'ta1-cadets-1-e5-official-2.bin.31.json',
 'ta1-cadets-1-e5-official-2.bin.31.json.1',
 'ta1-cadets-1-e5-official-2.bin.31.json.2',
 'ta1-cadets-1-e5-official-2.bin.32.json',
 'ta1-cadets-1-e5-official-2.bin.32.json.1',
 'ta1-cadets-1-e5-official-2.bin.32.json.2',
 'ta1-cadets-1-e5-official-2.bin.33.json',
 'ta1-cadets-1-e5-official-2.bin.33.json.1',
 'ta1-cadets-1-e5-official-2.bin.33.json.2',
 'ta1-cadets-1-e5-official-2.bin.34.json',
 'ta1-cadets-1-e5-official-2.bin.34.json.1',
 'ta1-cadets-1-e5-official-2.bin.34.json.2',
 'ta1-cadets-1-e5-official-2.bin.35.json',
 'ta1-cadets-1-e5-official-2.bin.35.json.1',
 'ta1-cadets-1-e5-official-2.bin.35.json.2',
 'ta1-cadets-1-e5-official-2.bin.36.json',
 'ta1-cadets-1-e5-official-2.bin.36.json.1',
 'ta1-cadets-1-e5-official-2.bin.36.json.2',
 'ta1-cadets-1-e5-official-2.bin.37.json',
 'ta1-cadets-1-e5-official-2.bin.37.json.1',
 'ta1-cadets-1-e5-official-2.bin.37.json.2',
 'ta1-cadets-1-e5-official-2.bin.38.json',
 'ta1-cadets-1-e5-official-2.bin.38.json.1',
 'ta1-cadets-1-e5-official-2.bin.38.json.2',
 'ta1-cadets-1-e5-official-2.bin.39.json',
 'ta1-cadets-1-e5-official-2.bin.39.json.1',
 'ta1-cadets-1-e5-official-2.bin.39.json.2',
 'ta1-cadets-1-e5-official-2.bin.3.json',
 'ta1-cadets-1-e5-official-2.bin.3.json.1',
 'ta1-cadets-1-e5-official-2.bin.3.json.2',
 'ta1-cadets-1-e5-official-2.bin.40.json',
 'ta1-cadets-1-e5-official-2.bin.40.json.1',
 'ta1-cadets-1-e5-official-2.bin.40.json.2',
 'ta1-cadets-1-e5-official-2.bin.41.json',
 'ta1-cadets-1-e5-official-2.bin.41.json.1',
 'ta1-cadets-1-e5-official-2.bin.41.json.2',
 'ta1-cadets-1-e5-official-2.bin.42.json',
 'ta1-cadets-1-e5-official-2.bin.42.json.1',
 'ta1-cadets-1-e5-official-2.bin.42.json.2',
 'ta1-cadets-1-e5-official-2.bin.43.json',
 'ta1-cadets-1-e5-official-2.bin.43.json.1',
 'ta1-cadets-1-e5-official-2.bin.43.json.2',
 'ta1-cadets-1-e5-official-2.bin.44.json',
 'ta1-cadets-1-e5-official-2.bin.44.json.1',
 'ta1-cadets-1-e5-official-2.bin.44.json.2',
 'ta1-cadets-1-e5-official-2.bin.45.json',
 'ta1-cadets-1-e5-official-2.bin.45.json.1',
 'ta1-cadets-1-e5-official-2.bin.45.json.2',
 'ta1-cadets-1-e5-official-2.bin.46.json',
 'ta1-cadets-1-e5-official-2.bin.46.json.1',
 'ta1-cadets-1-e5-official-2.bin.46.json.2',
 'ta1-cadets-1-e5-official-2.bin.47.json',
 'ta1-cadets-1-e5-official-2.bin.47.json.1',
 'ta1-cadets-1-e5-official-2.bin.47.json.2',
 'ta1-cadets-1-e5-official-2.bin.48.json',
 'ta1-cadets-1-e5-official-2.bin.48.json.1',
 'ta1-cadets-1-e5-official-2.bin.48.json.2',
 'ta1-cadets-1-e5-official-2.bin.49.json',
 'ta1-cadets-1-e5-official-2.bin.49.json.1',
 'ta1-cadets-1-e5-official-2.bin.49.json.2',
 'ta1-cadets-1-e5-official-2.bin.4.json',
 'ta1-cadets-1-e5-official-2.bin.4.json.1',
 'ta1-cadets-1-e5-official-2.bin.4.json.2',
 'ta1-cadets-1-e5-official-2.bin.50.json',
 'ta1-cadets-1-e5-official-2.bin.50.json.1',
 'ta1-cadets-1-e5-official-2.bin.50.json.2',
 'ta1-cadets-1-e5-official-2.bin.51.json',
 'ta1-cadets-1-e5-official-2.bin.51.json.1',
 'ta1-cadets-1-e5-official-2.bin.51.json.2',
 'ta1-cadets-1-e5-official-2.bin.52.json',
 'ta1-cadets-1-e5-official-2.bin.52.json.1',
 'ta1-cadets-1-e5-official-2.bin.52.json.2',
 'ta1-cadets-1-e5-official-2.bin.53.json',
 'ta1-cadets-1-e5-official-2.bin.53.json.1',
 'ta1-cadets-1-e5-official-2.bin.53.json.2',
 'ta1-cadets-1-e5-official-2.bin.54.json',
 'ta1-cadets-1-e5-official-2.bin.54.json.1',
 'ta1-cadets-1-e5-official-2.bin.54.json.2',
 'ta1-cadets-1-e5-official-2.bin.55.json',
 'ta1-cadets-1-e5-official-2.bin.55.json.1',
 'ta1-cadets-1-e5-official-2.bin.55.json.2',
 'ta1-cadets-1-e5-official-2.bin.56.json',
 'ta1-cadets-1-e5-official-2.bin.56.json.1',
 'ta1-cadets-1-e5-official-2.bin.56.json.2',
 'ta1-cadets-1-e5-official-2.bin.57.json',
 'ta1-cadets-1-e5-official-2.bin.57.json.1',
 'ta1-cadets-1-e5-official-2.bin.57.json.2',
 'ta1-cadets-1-e5-official-2.bin.58.json',
 'ta1-cadets-1-e5-official-2.bin.58.json.1',
 'ta1-cadets-1-e5-official-2.bin.58.json.2',
 'ta1-cadets-1-e5-official-2.bin.59.json',
 'ta1-cadets-1-e5-official-2.bin.59.json.1',
 'ta1-cadets-1-e5-official-2.bin.59.json.2',
 'ta1-cadets-1-e5-official-2.bin.5.json',
 'ta1-cadets-1-e5-official-2.bin.5.json.1',
 'ta1-cadets-1-e5-official-2.bin.5.json.2',
 'ta1-cadets-1-e5-official-2.bin.60.json',
 'ta1-cadets-1-e5-official-2.bin.60.json.1',
 'ta1-cadets-1-e5-official-2.bin.60.json.2',
 'ta1-cadets-1-e5-official-2.bin.61.json',
 'ta1-cadets-1-e5-official-2.bin.61.json.1',
 'ta1-cadets-1-e5-official-2.bin.61.json.2',
 'ta1-cadets-1-e5-official-2.bin.62.json',
 'ta1-cadets-1-e5-official-2.bin.62.json.1',
 'ta1-cadets-1-e5-official-2.bin.62.json.2',
 'ta1-cadets-1-e5-official-2.bin.63.json',
 'ta1-cadets-1-e5-official-2.bin.63.json.1',
 'ta1-cadets-1-e5-official-2.bin.63.json.2',
 'ta1-cadets-1-e5-official-2.bin.64.json',
 'ta1-cadets-1-e5-official-2.bin.64.json.1',
 'ta1-cadets-1-e5-official-2.bin.64.json.2',
 'ta1-cadets-1-e5-official-2.bin.65.json',
 'ta1-cadets-1-e5-official-2.bin.65.json.1',
 'ta1-cadets-1-e5-official-2.bin.65.json.2',
 'ta1-cadets-1-e5-official-2.bin.66.json',
 'ta1-cadets-1-e5-official-2.bin.66.json.1',
 'ta1-cadets-1-e5-official-2.bin.66.json.2',
 'ta1-cadets-1-e5-official-2.bin.67.json',
 'ta1-cadets-1-e5-official-2.bin.67.json.1',
 'ta1-cadets-1-e5-official-2.bin.67.json.2',
 'ta1-cadets-1-e5-official-2.bin.68.json',
 'ta1-cadets-1-e5-official-2.bin.68.json.1',
 'ta1-cadets-1-e5-official-2.bin.68.json.2',
 'ta1-cadets-1-e5-official-2.bin.69.json',
 'ta1-cadets-1-e5-official-2.bin.69.json.1',
 'ta1-cadets-1-e5-official-2.bin.69.json.2',
 'ta1-cadets-1-e5-official-2.bin.6.json',
 'ta1-cadets-1-e5-official-2.bin.6.json.1',
 'ta1-cadets-1-e5-official-2.bin.6.json.2',
 'ta1-cadets-1-e5-official-2.bin.70.json',
 'ta1-cadets-1-e5-official-2.bin.70.json.1',
 'ta1-cadets-1-e5-official-2.bin.70.json.2',
 'ta1-cadets-1-e5-official-2.bin.71.json',
 'ta1-cadets-1-e5-official-2.bin.71.json.1',
 'ta1-cadets-1-e5-official-2.bin.71.json.2',
 'ta1-cadets-1-e5-official-2.bin.72.json',
 'ta1-cadets-1-e5-official-2.bin.72.json.1',
 'ta1-cadets-1-e5-official-2.bin.72.json.2',
 'ta1-cadets-1-e5-official-2.bin.73.json',
 'ta1-cadets-1-e5-official-2.bin.73.json.1',
 'ta1-cadets-1-e5-official-2.bin.73.json.2',
 'ta1-cadets-1-e5-official-2.bin.74.json',
 'ta1-cadets-1-e5-official-2.bin.74.json.1',
 'ta1-cadets-1-e5-official-2.bin.74.json.2',
 'ta1-cadets-1-e5-official-2.bin.75.json',
 'ta1-cadets-1-e5-official-2.bin.75.json.1',
 'ta1-cadets-1-e5-official-2.bin.75.json.2',
 'ta1-cadets-1-e5-official-2.bin.76.json',
 'ta1-cadets-1-e5-official-2.bin.76.json.1',
 'ta1-cadets-1-e5-official-2.bin.76.json.2',
 'ta1-cadets-1-e5-official-2.bin.77.json',
 'ta1-cadets-1-e5-official-2.bin.77.json.1',
 'ta1-cadets-1-e5-official-2.bin.77.json.2',
 'ta1-cadets-1-e5-official-2.bin.78.json',
 'ta1-cadets-1-e5-official-2.bin.78.json.1',
 'ta1-cadets-1-e5-official-2.bin.78.json.2',
 'ta1-cadets-1-e5-official-2.bin.79.json',
 'ta1-cadets-1-e5-official-2.bin.79.json.1',
 'ta1-cadets-1-e5-official-2.bin.79.json.2',
 'ta1-cadets-1-e5-official-2.bin.7.json',
 'ta1-cadets-1-e5-official-2.bin.7.json.1',
 'ta1-cadets-1-e5-official-2.bin.7.json.2',
 'ta1-cadets-1-e5-official-2.bin.80.json',
 'ta1-cadets-1-e5-official-2.bin.80.json.1',
 'ta1-cadets-1-e5-official-2.bin.80.json.2',
 'ta1-cadets-1-e5-official-2.bin.81.json',
 'ta1-cadets-1-e5-official-2.bin.81.json.1',
 'ta1-cadets-1-e5-official-2.bin.81.json.2',
 'ta1-cadets-1-e5-official-2.bin.82.json',
 'ta1-cadets-1-e5-official-2.bin.82.json.1',
 'ta1-cadets-1-e5-official-2.bin.82.json.2',
 'ta1-cadets-1-e5-official-2.bin.83.json',
 'ta1-cadets-1-e5-official-2.bin.83.json.1',
 'ta1-cadets-1-e5-official-2.bin.83.json.2',
 'ta1-cadets-1-e5-official-2.bin.84.json',
 'ta1-cadets-1-e5-official-2.bin.84.json.1',
 'ta1-cadets-1-e5-official-2.bin.84.json.2',
 'ta1-cadets-1-e5-official-2.bin.85.json',
 'ta1-cadets-1-e5-official-2.bin.85.json.1',
 'ta1-cadets-1-e5-official-2.bin.85.json.2',
 'ta1-cadets-1-e5-official-2.bin.86.json',
 'ta1-cadets-1-e5-official-2.bin.86.json.1',
 'ta1-cadets-1-e5-official-2.bin.86.json.2',
 'ta1-cadets-1-e5-official-2.bin.87.json',
 'ta1-cadets-1-e5-official-2.bin.87.json.1',
 'ta1-cadets-1-e5-official-2.bin.87.json.2',
 'ta1-cadets-1-e5-official-2.bin.88.json',
 'ta1-cadets-1-e5-official-2.bin.88.json.1',
 'ta1-cadets-1-e5-official-2.bin.88.json.2',
 'ta1-cadets-1-e5-official-2.bin.89.json',
 'ta1-cadets-1-e5-official-2.bin.89.json.1',
 'ta1-cadets-1-e5-official-2.bin.89.json.2',
 'ta1-cadets-1-e5-official-2.bin.8.json',
 'ta1-cadets-1-e5-official-2.bin.8.json.1',
 'ta1-cadets-1-e5-official-2.bin.8.json.2',
 'ta1-cadets-1-e5-official-2.bin.90.json',
 'ta1-cadets-1-e5-official-2.bin.90.json.1',
 'ta1-cadets-1-e5-official-2.bin.90.json.2',
 'ta1-cadets-1-e5-official-2.bin.91.json',
 'ta1-cadets-1-e5-official-2.bin.91.json.1',
 'ta1-cadets-1-e5-official-2.bin.91.json.2',
 'ta1-cadets-1-e5-official-2.bin.92.json',
 'ta1-cadets-1-e5-official-2.bin.92.json.1',
 'ta1-cadets-1-e5-official-2.bin.92.json.2',
 'ta1-cadets-1-e5-official-2.bin.93.json',
 'ta1-cadets-1-e5-official-2.bin.93.json.1',
 'ta1-cadets-1-e5-official-2.bin.93.json.2',
 'ta1-cadets-1-e5-official-2.bin.94.json',
 'ta1-cadets-1-e5-official-2.bin.94.json.1',
 'ta1-cadets-1-e5-official-2.bin.94.json.2',
 'ta1-cadets-1-e5-official-2.bin.95.json',
 'ta1-cadets-1-e5-official-2.bin.95.json.1',
 'ta1-cadets-1-e5-official-2.bin.95.json.2',
 'ta1-cadets-1-e5-official-2.bin.96.json',
 'ta1-cadets-1-e5-official-2.bin.96.json.1',
 'ta1-cadets-1-e5-official-2.bin.96.json.2',
 'ta1-cadets-1-e5-official-2.bin.97.json',
 'ta1-cadets-1-e5-official-2.bin.97.json.1',
 'ta1-cadets-1-e5-official-2.bin.97.json.2',
 'ta1-cadets-1-e5-official-2.bin.98.json',
 'ta1-cadets-1-e5-official-2.bin.98.json.1',
 'ta1-cadets-1-e5-official-2.bin.98.json.2',
 'ta1-cadets-1-e5-official-2.bin.99.json',
 'ta1-cadets-1-e5-official-2.bin.99.json.1',
 'ta1-cadets-1-e5-official-2.bin.99.json.2',
 'ta1-cadets-1-e5-official-2.bin.9.json',
 'ta1-cadets-1-e5-official-2.bin.9.json.1',
 'ta1-cadets-1-e5-official-2.bin.9.json.2',
 'ta1-cadets-1-e5-official-2.bin.json',
 'ta1-cadets-1-e5-official-2.bin.json.1',
 'ta1-cadets-1-e5-official-2.bin.json.2'
]


def store_netflow(file_path, cur, connect):
    # Parse data from logs
    netobjset = set()
    netobj2hash = {}
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if "avro.cdm20.NetFlowObject" in line:
                    try:
                        res=re.findall('NetFlowObject":{"uuid":"(.*?)"(.*?)"localAddress":{"string":"(.*?)"},"localPort":{"int":(.*?)},"remoteAddress":{"string":"(.*?)"},"remotePort":{"int":(.*?)}',line)[0]

                        nodeid=res[0]
                        srcaddr=res[2]
                        srcport=res[3]
                        dstaddr=res[4]
                        dstport=res[5]

                        nodeproperty = srcaddr + "," + srcport + "," + dstaddr + "," + dstport
                        netobj2hash[nodeid] = [nodeproperty]
                    except:
                        pass

    # Store data into database
    datalist = []
    for i in netobj2hash.keys():
        datalist.append([i] + netobj2hash[i][0].split(","))

    sql = '''insert into netflow_node_table
                        values %s
            '''

    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()
    del netobj2hash
    del datalist

def store_subject(file_path, cur, connect):
    scusess_count = 0
    fail_count = 0
    subject_objset = set()
    subject_obj2hash = {}  #
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if "schema.avro.cdm20.Event" in line:
                    subject_uuid = re.findall(
                        '"subject":{"com.bbn.tc.schema.avro.cdm20.UUID":"(.*?)"},(.*?)"exec":"(.*?)",', line)
                    try:
                        subject_obj2hash[subject_uuid[0][0]] = subject_uuid[0][-1]
                        scusess_count += 1
                    except:
                        try:
                            subject_obj2hash[subject_uuid[0][0]] = "null"
                        except:
                            pass
                        fail_count += 1
    # Store into database
    datalist = []
    for i in subject_obj2hash.keys():
        datalist.append([i] + [subject_obj2hash[i]])
    sql = '''insert into subject_node_table
                             values %s
                '''
    ex.execute_values(cur, sql, datalist, page_size=10000)
    connect.commit()

def store_file(file_path, cur, connect):
    file_node = set()
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if "schema.avro.cdm20.FileObject" in line:
                    Object_uuid = re.findall('{"com.bbn.tc.schema.avro.cdm20.FileObject":{"uuid":"(.*?)"', line)
                    try:
                        file_node.add(Object_uuid[0])
                    except:
                        print(line)

    file_obj2hash = {}
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if "schema.avro.cdm20.Event" in line:
                    predicateObject_uuid = re.findall('"predicateObject":{"com.bbn.tc.schema.avro.cdm20.UUID":"(.*?)"},', line)
                    if len(predicateObject_uuid) > 0:
                        if predicateObject_uuid[0] in file_node:
                            if '"predicateObjectPath":null,' not in line and '<unknown>' not in line:  # predicateObjectPath不能为空
                                path_name = re.findall('"predicateObjectPath":{"string":"(.*?)"', line)
                                file_obj2hash[predicateObject_uuid[0]] = path_name

    datalist=[]
    for i in file_obj2hash.keys():
        datalist.append([i]+[file_obj2hash[i][0]])

    sql = '''insert into file_node_table
                         values %s
            '''
    ex.execute_values(cur,sql, datalist,page_size=10000)
    connect.commit()


def create_node_list(cur, connect):
    node_list = {}

    # file
    sql = """
    select * from file_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()

    for i in records:
        node_list[i[0]] = ["file", i[-1]]
    file_uuidList = []
    for i in records:
        file_uuidList.append(i[0])

    # subject
    sql = """
    select * from subject_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()
    for i in records:
        node_list[i[0]] = ["subject", i[-1]]
    subject_uuidList = []
    for i in records:
        subject_uuidList.append(i[0])

    # netflow
    sql = """
    select * from netflow_node_table;
    """
    cur.execute(sql)
    records = cur.fetchall()
    for i in records:
        node_list[i[0]] = ["netflow", i[-4] + ":" + i[-3] + "->" + i[-2] + ":" + i[-1]]

    net_uuidList = []
    for i in records:
        net_uuidList.append(i[0])

    node_list_database = []
    node_index = 0
    for i in node_list:
        node_list_database.append([i] + node_list[i] + [node_index])
        node_index += 1

    sql = '''insert into node2id
                         values %s
            '''
    ex.execute_values(cur, sql, node_list_database, page_size=10000)
    connect.commit()

    sql = "select * from node2id ORDER BY index_id;"
    cur.execute(sql)
    rows = cur.fetchall()
    nodeid2msg = {}
    for i in rows:
        nodeid2msg[i[0]] = i[-1]

    return nodeid2msg, subject_uuidList, file_uuidList, net_uuidList

def store_event(file_path, cur, connect, reverse, nodeid2msg, subject_uuidList, file_uuidList, net_uuidList):
    valid_subjects = set(subject_uuidList)
    valid_allnodes = set(subject_uuidList + file_uuidList + net_uuidList)

    subject_uuid_pattern = re.compile('"subject":{"com.bbn.tc.schema.avro.cdm20.UUID":"(.*?)"}')
    predicateObject_uuid_pattern = re.compile('"predicateObject":{"com.bbn.tc.schema.avro.cdm20.UUID":"(.*?)"}')
    type_pattern = re.compile('"type":"(.*?)"')
    timestamp_pattern = re.compile('"timestampNanos":(.*?),')

    datalist=[]
    edge_type=set()
    total_event_count=0
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in (f):
                if '{"datum":{"com.bbn.tc.schema.avro.cdm20.Event"' in line:
                    total_event_count+=1
#                     print(line)
                    relation_type_match = type_pattern.search(line)
                    if relation_type_match:
                        relation_type = relation_type_match.group(1)
                        subject_uuid_match = subject_uuid_pattern.search(line)
                        predicateObject_uuid_match = predicateObject_uuid_pattern.search(line)
                        if subject_uuid_match and predicateObject_uuid_match:
                            subject_uuid = subject_uuid_match.group(1)
                            predicateObject_uuid = predicateObject_uuid_match.group(1)
                            if subject_uuid in valid_subjects and predicateObject_uuid in valid_allnodes:
                                time_rec_match = timestamp_pattern.search(line)
                                if time_rec_match:
                                    time_rec = int(time_rec_match.group(1))
                                    subjectId = subject_uuid
                                    objectId = predicateObject_uuid
                                    if relation_type in reverse:
                                        datalist.append([objectId,nodeid2msg[objectId],relation_type,subjectId,nodeid2msg[subjectId],time_rec])
                                    else:
                                        datalist.append([subjectId,nodeid2msg[subjectId],relation_type,objectId,nodeid2msg[objectId],time_rec])
        sql = '''insert into event_table
                                 values %s
                    '''
        ex.execute_values(cur, sql, datalist, page_size=10000)
        datalist.clear()

    connect.commit()
    print("total_event_count:",total_event_count)


if __name__ == "__main__":
    cur, connect = init_database_connection()

    print("Processing netflow data")
    store_netflow(file_path=raw_dir, cur=cur, connect=connect)

    print("Processing subject data")
    store_subject(file_path=raw_dir, cur=cur, connect=connect)
 
    print("Processing file data")
    store_file(file_path=raw_dir, cur=cur, connect=connect)

    print("Extracting the node list")
    nodeid2msg, subject_uuidList, file_uuidList, net_uuidList = create_node_list(cur=cur, connect=connect)

    print("Processing the events")
    store_event(
        file_path=raw_dir,
        cur=cur,
        connect=connect,
        reverse=edge_reversed,
        nodeid2msg=nodeid2msg,
        subject_uuidList=subject_uuidList,
        file_uuidList=file_uuidList,
        net_uuidList=net_uuidList
    )
