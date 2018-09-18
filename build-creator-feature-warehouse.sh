#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date +%Y-%m-%d`
yesterdayStr=`date -d "yesterday" +%Y-%m-%d`

userFollowCountInput="/db_dump/music_ndir/Music_UserDetailInfo"
eventMeta90dInput="/db_dump/music_ndir/Music_EventMeta_180d"
followRateInput="/user/ndir/music_recommend/event/stat/follow_rate/"$yesterdayStr
creatorVectorFromFollowInput="/user/ndir/music_recommend/feed_video/follow/als_direct_follow_model.all/itemFactors"
output="/user/ndir/music_recommend/event/creator_feature_warehouse/dt="$dateStr
outputHistory="/user/ndir/music_recommend/event/creator_feature_warehouse_history"

/data/music_useraction/wait_hdfs_file.sh $userFollowCountInput/_SUCCESS
/data/music_useraction/wait_hdfs_file_not_today.sh $eventMeta90dInput/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh $followRateInput/_SUCCESS
/data/music_useraction/wait_hdfs_file_not_today.sh $creatorVectorFromFollowInput/_SUCCESS

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.BuildCreatorFeatureWarehouse \
--master yarn \
--deploy-mode cluster \
--executor-memory 10g \
--name "build creator feature warehouse" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-dateStr $dateStr \
-userFollowCountInput $userFollowCountInput \
-eventMeta90dInput $eventMeta90dInput \
-followRateInput $followRateInput \
-creatorVectorFromFollowInput $creatorVectorFromFollowInput \
-output $output

