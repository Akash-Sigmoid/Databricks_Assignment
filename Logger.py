# Databricks notebook source
import logging
import datetime
import time

date=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
name='/tmp/log'+date+'.log'
logger = logging.getLogger('pipeline_logger')
f_handler = logging.FileHandler(name,mode='a')
logger.setLevel(logging.DEBUG)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)
