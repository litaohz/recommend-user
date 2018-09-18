#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

dateStr=`date +%Y-%m-%d`

matchResultInput="/user/ndir/music_recommend/event/creator_match_result_all"
modelPath="/user/ndir/music_recommend/event/model/creator_model/baseline"
creatorFeatureInput="/user/ndir/music_recommend/event/creator_feature_warehouse/dt="$dateStr
userFeatureInput="/user/ndir/music_recommend/event/user_feature_warehouse_wb/dt="$dateStr
tmpOutput="/user/ndir/music_recommend/event/tmp/creator_prediction"
output="/user/ndir/music_recommend/event/creator_reranked_result"

/data/music_useraction/wait_hdfs_file.sh $matchResultInput/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh $creatorFeatureInput/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh $userFeatureInput/_SUCCESS

hadoop fs -rm -r $tmpOutput
hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.RerankCreators \
--master yarn \
--deploy-mode cluster \
--executor-cores 2 \
--executor-memory 16g \
--conf spark.sql.shuffle.partitions=4000 \
--name "rerank match result" \
--queue music_feed.sla \
user-recommend-1.0-SNAPSHOT.jar \
-matchResultInput $matchResultInput -modelPath $modelPath \
-creatorFeatureInput $creatorFeatureInput \
-userFeatureInput $userFeatureInput \
-tmpOutput $tmpOutput \
-needFinalOutput -output $output

