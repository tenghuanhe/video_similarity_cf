#!/usr/bin/python
import random

datafile = open('user_video_pref.txt', 'w+')

for i in range(1, 10000):
  user_id = random.randint(1, 100)
  video_id = random.randint(1, 100)
  pref = random.random()
  datafile.write(str(user_id) + ' ' + str(video_id) + ' ' + str(pref) + '\n')
datafile.close()
