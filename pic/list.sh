#!/bin/sh

cd /home/ubuntu/CS553_Assignment4/pic
wget -i pic.txt
x=1; for i in *jpg; do counter=$(printf %d $x); ln -s "$i" /Users/WayneHu/Desktop/pic/pic"$counter".jpg; x=$(($x+1)); done
ffmpeg -i 'pic%d.jpg' -c:v libx264 -preset ultrafast -qp 0 -filter:v "setpts=25.5*PTS" out.mkv
