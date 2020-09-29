#!/bin/sh

HERE="/home/cmswriter/scripts"
TOP="/cms/phedex/store"
CORRUPT_FILES=`mktemp -t FSCK.txt.XXXXXXX` 
hdfs fsck ${TOP} 2>&1 | grep CORRUPT | awk -F\: '{print $1}' | sort | uniq > $CORRUPT_FILES


check() {
  DIR=$1
  ALIAS=$2
  n_corrupt=`cat $CORRUPT_FILES | grep $DIR | grep -v "The filesystem" | wc -l`
  if [ ! -z $ALIAS ] ; then DIR=$ALIAS ; fi
  printf "%-40s %8i\n" $DIR $n_corrupt
  curl -i -XPOST 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db' -H "Authorization: Token hadoop_writer:Hadoop3r" --data-raw "corrupt_files,dir=$DIR value=$n_corrupt"
  return $n_corrupt
}


compile_stats() {
  SUBDIRS=`ls /hadoop${TOP} | awk '{print $0}'`
  printf "%-20s %28s\n" "Subdirectory" "Corrupt Files"
  echo "-------------------------------------------------"
  for SUBDIR in $SUBDIRS ; do
    check $TOP/$SUBDIR
  done
  echo "-------------------------------------------------"
  check $TOP "TOTAL"
  echo 
  printf "%-20s %28s\n" "Critical Samples" "Corrupt Files"
  echo "-------------------------------------------------"
  check "/cms/phedex/store/PhEDEx_LoadTest07/LoadTest07_Debug_UCSD" "LoadTest"
  lt_corrupt=$?
  check "/cms/phedex/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_7_0_4_START70_V7-v1" "HammerCloud"
  hc_corrupt=$?
  rc=0
  if [ $lt_corrupt -ne 0 ] ; then rc=1 ; fi
  if [ $hc_corrupt -ne 0 ] ; then rc=1 ; fi
  return $rc
}


OUTPUT_FILE=`mktemp -t RESULTS.txt.XXXXXXX`
compile_stats > $OUTPUT_FILE
rc=$?

SUBJECT="Check of corrupt files in hadoop"
if [ $rc -ne 0 ] ; then
  SUBJECT="CRITICAL corrupt files in hadoop!"
  printf "\n%-20s\n\n" "Critical Files:" >> $OUTPUT_FILE
  URL1="/cms/phedex/store/PhEDEx_LoadTest07/LoadTest07_Debug_UCSD"
  grep $URL1 $CORRUPT_FILES >> $OUTPUT_FILE
  URL2="/cms/phedex/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_7_0_4_START70_V7-v1"
  grep $URL2 $CORRUPT_FILES >> $OUTPUT_FILE
fi


#unfixable_datasets() {
#  DDM=`mktemp -t DDM.txt.XXXXXXX`
#  URL="http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/T2_US_UCSD/RemainingDatasets.txt"
#  curl -s -k -o $DDM $URL
#  DATASETS=`cat $DDM | awk '($3==1){print $5}'`
#  rm $DDM
#  for DATASET in $DATASETS; do
#    NAME=`echo $DATASET | awk -F\/ '{print $2}'`
#    CAMPAIGN=`echo $DATASET | awk -F\/ '{print $3}' | awk -F\- '{print $1}'`
#    DETAILS=`echo $DATASET | awk -F\/ '{print $3}' | awk -F\- '{print $2}'`
#    FORMAT=`echo $DATASET | awk -F\/ '{print $4}'`
#    cat $CORRUPT_FILES | grep $NAME | grep $CAMPAIGN | grep $DETAILS | grep $FORMAT
#  done
#  return
#}
#
#UNFIX=`mktemp -t UNFIX.txt.XXXXXXX`
#unfixable_datasets | sort | uniq > $UNFIX
#N_UNFIX=`cat $UNFIX | wc -l`
#printf "\n%-20s %8i\n\n" "Un-healable Files:" $N_UNFIX >> $OUTPUT_FILE
#cat $UNFIX >> $OUTPUT_FILE
#rm $UNFIX

cat $OUTPUT_FILE | mail -s "$SUBJECT" "rgao@ucsd.edu"

cp $OUTPUT_FILE $HERE
cp $CORRUPT_FILES $HERE

rm $OUTPUT_FILE
rm $CORRUPT_FILES

exit $rc

#POST-PROCESSING:
# for file in `cat RESULTS.txt.3RPXS6H` ; do hdfs fsck $file >> /dev/null 2>&1 ; rc=$? ; if [ $rc != 1 ] ; then echo HEALTHY FILE! $file ; break ; fi; done
