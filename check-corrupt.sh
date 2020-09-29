#!/bin/sh

HERE="/home/cmswriter/scripts"
TOP="/cms/phedex/store"
CORRUPT_FILES=`mktemp -t FSCK.txt.XXXXXXX` 
hdfs fsck ${TOP} 2>&1 | grep CORRUPT | awk -F\: '{print $1}' | sort | uniq > $CORRUPT_FILES


TOKEN=`cat conf.json | grep Authorization | sed 's/"//g' | sed -e 's/^[[:space:]]*//'`

check() {
  DIR=$1
  ALIAS=$2
  n_corrupt=`cat $CORRUPT_FILES | grep $DIR | grep -v "The filesystem" | wc -l`
  if [ ! -z $ALIAS ] ; then DIR=$ALIAS ; fi
  printf "%-40s %8i\n" $DIR $n_corrupt
  curl -i -XPOST 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db' -H "$TOKEN" -d "corrupt_files,dir=$DIR value=$n_corrupt"
  return $n_corrupt
}


compile_stats() {
  SUBDIRS=`ls /hadoop${TOP} | awk '{print $0}'`
  for SUBDIR in $SUBDIRS ; do
    check $TOP/$SUBDIR
  done
  check $TOP "TOTAL"
  check "/cms/phedex/store/PhEDEx_LoadTest07/LoadTest07_Debug_UCSD" "LoadTest"
  lt_corrupt=$?
  check "/cms/phedex/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_7_0_4_START70_V7-v1" "HammerCloud"
  hc_corrupt=$?
  rc=0
  if [ $lt_corrupt -ne 0 ] ; then rc=1 ; fi
  if [ $hc_corrupt -ne 0 ] ; then rc=1 ; fi
  return $rc
}

compile_stats

rc=$?
exit $rc

