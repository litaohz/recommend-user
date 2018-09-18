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
#/data/music_useraction/wait_hdfs_file.sh $preLogPlayendInput/../_SUCCESS
for ((i=startIterDay;i<=endIterDay;i++));
do
  past=`date -d "$i days ago" +%Y-%m-%d`
  sampleOneDay=$sampleBasePath"/dt="$past
  sampleInput=$sampleInput","$sampleOneDay
  creatorFeatureOneDay=$creatorFeatureBasePath"/dt="$past
  creatorFeatureInput=$creatorFeatureInput","$creatorFeatureOneDay
done

echo $sampleInput
echo $creatorFeatureInput

output="/user/ndir/music_recommend/event/creator_dataset_popular/"$dateStr

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.popular.CreatorDataSet \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "build training dataset" \
--queue music_feed \
user-recommend-2.0-SNAPSHOT.jar \
-sampleBasePath $sampleBasePath -sampleInput $sampleInput \
-creatorFeatureBasePath $creatorFeatureBasePath -creatorFeatureInput $creatorFeatureInput \
-output $output

