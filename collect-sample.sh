#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date -d "$1 day ago" +%Y-%m-%d`
echo $dateStr

biUserActionInput="/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt="$dateStr

echo $biUserActionInput

output="/user/ndir/music_recommend/event/creator_sample/dt="$dateStr

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.CollectSample \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "collect sample for recommend creators" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-biUserActionInput $biUserActionInput \
-dateStr $dateStr \
-output $output

