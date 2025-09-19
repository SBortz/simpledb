#!/bin/bash

db_set () {
  echo "$1,$2" >> database
}

db_get () {
  grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
}

bulk_insert() {
  for i in $(seq 1 100000000); do
    printf 'key%d,value%d\n' "$i" "$i"
  done >> database
}

bulk_insert_awk() {
  awk -v n=1000000000 'BEGIN {
    for (i=1; i<=n; i++) printf "key%d,value%d\n", i, i
  }' >> database
}

db_set_bin() {
  local key=$1
  local value=$2
  local len=${#value}

  # 4 Byte key (little endian), 4 Byte length, dann value
  printf "%08x" "$key" | xxd -r -p >> database.bin
  printf "%08x" "$len" | xxd -r -p >> database.bin
  printf "%s" "$value" >> database.bin
}
db_get_bin() {
  local search=$1 offset=0 filesize key len
  local last_val=$(mktemp)
  : > "$last_val"
  filesize=$(stat -c%s database.bin)

  while [ $offset -lt $filesize ]; do
    dd if=database.bin of=header.bin bs=1 skip=$offset count=8 status=none
    local h
    h=$(xxd -p -c 8 header.bin)
    key=$(( 0x${h:0:8} ))
    len=$(( 0x${h:8:8} ))
    offset=$((offset+8))
    if [ "$key" -eq "$search" ]; then
      dd if=database.bin of="$last_val" bs=1 skip=$offset count=$len status=none
    fi
    offset=$((offset+len))
  done

  if [ -s "$last_val" ]; then
    cat "$last_val"
    printf '\n'
  fi
  rm -f "$last_val"
}
