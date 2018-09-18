#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile
cd /data/music_useraction/socialEvent/recommend_creator

# 计算多场景的关注率
./get-follow-rate-increment.sh
./get-follow-rate.sh

# 构建创作者特征仓库
./build-creator-feature-warehouse.sh

# 构建用户特征仓库
./build-user-feature-warehouse.sh

