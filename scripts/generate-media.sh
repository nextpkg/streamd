#!/bin/sh
set -eu

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_directory=$(dirname -- "$script_directory")
output_directory=${1:-"$project_directory/testdata/generated"}
duration=${DURATION_SECONDS:-6}

command -v ffmpeg >/dev/null
command -v ffprobe >/dev/null
mkdir -p "$output_directory"
ffmpeg -version > "$output_directory/ffmpeg-version.txt"

for variant in regular bframes; do
  bframes=0
  if [ "$variant" = bframes ]; then bframes=2; fi
  ffmpeg -nostdin -hide_banner -loglevel error -y \
    -f lavfi -i 'testsrc2=size=320x180:rate=25' \
    -f lavfi -i 'sine=frequency=1000:sample_rate=44100' \
    -t "$duration" -map 0:v:0 -map 1:a:0 \
    -c:v libx264 -preset veryfast -threads:v 1 -pix_fmt yuv420p \
    -profile:v main -bf "$bframes" -g 50 -keyint_min 50 -sc_threshold 0 \
    -b:v 300k -c:a aac -b:a 64k -ar 44100 -ac 2 \
    -map_metadata -1 -fflags +bitexact -flags:v +bitexact -flags:a +bitexact \
    -f flv "$output_directory/$variant.flv"
  ffprobe -v error -count_packets -count_frames \
    -show_entries 'stream=codec_name,codec_type,width,height,has_b_frames,sample_rate,channels,nb_read_packets,nb_read_frames:format=duration,size' \
    -of json "$output_directory/$variant.flv" > "$output_directory/$variant.json"
  ffmpeg -nostdin -hide_banner -loglevel error -xerror \
    -i "$output_directory/$variant.flv" -map 0:v:0 -map 0:a:0 -f null -
done

(cd "$output_directory" && shasum -a 256 regular.flv bframes.flv > SHA256SUMS)
printf 'Generated and independently decoded both fixtures in %s\n' "$output_directory"
