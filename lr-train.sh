#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date -d "$day day ago" +%Y-%m-%d`

datasetInput="/user/ndir/music_recommend/event/creator_dataset/2018-07-29"
modelOutput="/user/ndir/music_recommend/event/model/creator_model/baseline"
output="/user/ndir/music_recommend/event/creator_prediction"

hadoop fs -rm -r $modelOutput
hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.TrainLRModel \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "train lr model for creator" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-datasetInput $datasetInput \
-modelOutput $modelOutput \
-output $output

