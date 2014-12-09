#!/bin/sh

cd /home/ubuntu/CS553_Assignment4/pic
wget -i pic.txt >> /home/ubuntu/Log$1.txt
x=1; for i in *jpg; do counter=$(printf %d $x); ln -s "$i" /home/ubuntu/CS553_Assignment4/pic/pic"$counter".jpg; x=$(($x+1)); done
ffmpeg -i 'pic%d.jpg' -c:v libx264 -preset ultrafast -qp 0 -filter:v "setpts=25.5*PTS" out$1.mkv >> /home/ubuntu/Log$1.txt
rm *.jpg*
