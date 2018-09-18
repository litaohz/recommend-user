#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

if [ ! -z $1 ]
then
  day=$1
else
  day=1
fi

startIterDay=$((day+1))
endIterDay=$((day+4))
echo $day
echo $startIterDay
echo $endIterDay

dateStr=`date -d "$day day ago" +%Y-%m-%d`

sampleBasePath="/user/ndir/music_recommend/event/creator_sample"
sampleInput=$sampleBasePath"/dt="$dateStr
creatorFeatureBasePath="/user/ndir/music_recommend/event/creator_feature_warehouse"
creatorFeatureInput=$creatorFeatureBasePath"/dt="$dateStr
userFeatureBasePath="/user/ndir/music_recommend/event/user_feature_warehouse_wb"
userFeatureInput=$userFeatureBasePath"/dt="$dateStr
#/data/music_useraction/wait_hdfs_file.sh $preLogPlayendInput/../_SUCCESS
for ((i=startIterDay;i<=endIterDay;i++));
do
  past=`date -d "$i days ago" +%Y-%m-%d`
  sampleOneDay=$sampleBasePath"/dt="$past
  sampleInput=$sampleInput","$sampleOneDay
  creatorFeatureOneDay=$creatorFeatureBasePath"/dt="$past
  creatorFeatureInput=$creatorFeatureInput","$creatorFeatureOneDay
  userFeatureOneDay=$userFeatureBasePath"/dt="$past
  userFeatureInput=$userFeatureInput","$userFeatureOneDay
done

echo $sampleInput
echo $creatorFeatureInput
echo $userFeatureInput

output="/user/ndir/music_recommend/event/creator_dataset/"$dateStr

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.BuildDataset \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "build training dataset" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-sampleBasePath $sampleBasePath -sampleInput $sampleInput \
-creatorFeatureBasePath $creatorFeatureBasePath -creatorFeatureInput $creatorFeatureInput \
-userFeatureBasePath $userFeatureBasePath -userFeatureInput $userFeatureInput \
-output $output

