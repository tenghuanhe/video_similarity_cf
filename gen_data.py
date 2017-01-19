#!/usr/bin/python
import random

datafile = open('user_video_pref.txt', 'w+')

for i in range(1, 1000000):
  user_id = random.randint(1, 1000)
  video_id = random.randint(1, 1000)
  pref = random.random()
  datafile.write(str(user_id) + ' ' + str(video_id) + ' ' + str(pref) + '\n')
datafile.close()
