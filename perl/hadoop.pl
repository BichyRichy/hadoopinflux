#!/usr/bin/perl
for (my $i=0; $i < $Config{'General.sections'}; $i++) {
    my $section = $Config{"$i.Name"};
    open(DU,"$hadoop fs -du $duarg \'$Config{\"$i.Path\"}\'|") or die "Could not run $!\n";
    while (<DU>) {
        chomp;
        my ($size,$usage,$path) = split;
        insertsection($section,$path,$size,$curdate);
    }
    close DU;
}

# This is just an overall collection of stats about the storage
open(DF,"$hadoop fs -df |") or die "Could not run $!\n";
my $trash = <DF>;
my $goodies;
chomp ($goodies = <DF>);
my ($fs,$size,$used,$avail,$useperc,$trash) = split(/\s+/,$goodies);
insertusage($fs,$size,$used,$avail,$useperc);
close DF;

# Finally, collecting the missing blocks and live nodes, I recommend starting here as it is very straightforward and is very useful data to know.
open(REPORT,"/usr/bin/hdfs dfsadmin -report|") || die "Could not run $!\n";
my $missing;
my $nodes;
#while (<REPORT>) {
    chomp;
        if ( /Missing blocks:\s(\d+)/ ) {
                $missing = $1;
                    }
                        if ( /Live datanodes\s\((\d+)\):/ ) {
                                $nodes = $1;
                                    }
