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
endIterDay=$((day+6))
echo $day
echo $startIterDay
echo $endIterDay

dateStr=`date -d "$day day ago" +%Y-%m-%d`

baseStatBasePath="/user/ndir/music_recommend/event/base_stat"
baseStatInput=$baseStatBasePath"/dt="$dateStr
#/data/music_useraction/wait_hdfs_file.sh $preLogPlayendInput/../_SUCCESS
for ((i=startIterDay;i<=endIterDay;i++));
do
  past=`date -d "$i days ago" +%Y-%m-%d`
  baseStatOneDay=$baseStatBasePath"/dt="$past
  baseStatInput=$baseStatInput","$baseStatOneDay
done

echo $baseStatInput

output="/user/ndir/music_recommend/event/stat/follow_rate/"$dateStr

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.event.stat.GetFollowRate \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--name "compute creator follow rate" \
--queue music_feed \
user-recommend-1.0-SNAPSHOT.jar \
-baseStatBasePath $baseStatBasePath \
-baseStatInput $baseStatInput \
-dateStr $dateStr \
-output $output

