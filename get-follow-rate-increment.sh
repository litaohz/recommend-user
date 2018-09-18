#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date -d "$1 day ago" +%Y-%m-%d`
echo $dateStr

biUserActionInput="/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt="$dateStr

echo $biUserActionInput

/data/scripts/wait_hdfs_file_update_end.sh $biUserActionInput

output="/user/ndir/music_recommend/event/base_stat/dt="$dateStr

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.stat.GetFollowStatIncrement \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "prepare data for itembased-CF" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-biUserActionInput $biUserActionInput \
-dateStr $dateStr \
-output $output

