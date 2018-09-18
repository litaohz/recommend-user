#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date -d "$day day ago" +%Y-%m-%d`

datasetInput="/user/ndir/music_recommend/event/creator_dataset_popular/2018-08-16/"
modelOutput="/user/ndir/music_recommend/event/model/creator_popular_model/baseline1"
output="/user/ndir/music_recommend/event/creator_popular_prediction1"

hadoop fs -rm -r $modelOutput
hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.popular.TrainPopularModel \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "train lr model for creator" \
--queue music_feed \
user-recommend-2.0-SNAPSHOT.jar \
-datasetInput $datasetInput \
-modelOutput $modelOutput \
-output $output

