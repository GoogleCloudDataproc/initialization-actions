#!/usr/bin/env python3 
# 
# Copyright 2025 Google LLC and contributors
# 
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# 
#      http://www.apache.org/licenses/LICENSE-2.0 
# 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS-IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License. 
# 
import matplotlib.pyplot as plt
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, StorageLevel
from tqdm import tqdm
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
import pyspark.sql.functions as f
import nltk

spark = SparkSession.builder.appName("spark-rapids").getOrCreate()

#from utils import SimpleTimer, ResultsLogger, visualize_data 

conf = (SparkConf().setMaster("local[*]")
                   .setAppName("SparkVectorizer")
                   .set('spark.driver.memory', '300G')
                   .set('spark.driver.maxResultSize', '20G')
                   .set('spark.network.timeout', '7200s')
        )

sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("FATAL")
spark = SparkSession(sc)
print(sc._conf.getAll()) # check context settings 

x = np.linspace(0, 3*np.pi, 500)
plt.plot(x, np.sin(x**2))
plt.title('A simple chirp');
