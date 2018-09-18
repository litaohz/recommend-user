#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date +%Y-%m-%d`

matchResultInput="/user/ndir/music_recommend/event/creator_pool/"
modelPath="/user/ndir/music_recommend/event/model/creator_popular_model/baseline1"
creatorFeatureInput="/user/ndir/music_recommend/event/creator_feature_warehouse/dt="$dateStr
tmpOutput="/user/ndir/music_recommend/event/tmp/creator_popular_prediction"
output="/user/ndir/music_recommend/event/creator_popular_reranked_result"

/data/music_useraction/wait_hdfs_file_no_time.sh $matchResultInput/_SUCCESS
/data/music_useraction/wait_hdfs_file_no_time.sh $creatorFeatureInput/_SUCCESS
hadoop fs -rm -r $tmpOutput
hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.popular.RankPopularCreators \
--master yarn \
--deploy-mode cluster \
--executor-cores 2 \
--executor-memory 16g \
--name "rerank match result" \
--queue music_feed.sla \
user-recommend-2.0-SNAPSHOT.jar \
-matchResultInput $matchResultInput -modelPath $modelPath \
-creatorFeatureInput $creatorFeatureInput \
-tmpOutput $tmpOutput \
-needFinalOutput -output $output

