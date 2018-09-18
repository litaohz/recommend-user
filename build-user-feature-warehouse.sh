#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date +%Y-%m-%d`
yesterdayStr=`date -d "yesterday" +%Y-%m-%d`

userVectorFromFollowInput="/user/ndir/music_recommend/feed_video/follow/als_direct_follow_model.all/userFactors"
output="/user/ndir/music_recommend/event/user_feature_warehouse_wb/dt="$dateStr

/data/music_useraction/wait_hdfs_file_not_today.sh $userVectorFromFollowInput/_SUCCESS

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.BuildUserFeatureWarehouse \
--master yarn \
--deploy-mode cluster \
--executor-memory 10g \
--name "build user feature warehouse" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-dateStr $dateStr \
-userVectorFromFollowInput $userVectorFromFollowInput \
-output $output

