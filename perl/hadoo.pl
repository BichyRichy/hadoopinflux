#!/usr/bin/perl
my %dirs = qw(CMS /cms/* Phedex /cms/phedex/store/* Store /cms/store/* Group /cms/store/group/* Users /cms/store/user/* All /*);

for (keys %dirs) {
    my $section = $_;
    print "\'$dirs{$_}\'|";
    exit 42;
    open(DU,"hadoop fs -du $duarg \'$dirs{$_}\'|") or die "Could not run $!\n";
    while (<DU>) {
        chomp;
        my ($size,$usage,$path) = split;
        # insertsection($section,$path,$size,$curdate);
        my @list = ($section,$path,$size,$curdate);
	print "@list\n"
    }
    close DU;
}

# This is just an overall collection of stats about the storage

open(DF,"hadoop fs -df |") or die "Could not run $!\n";
my $trash = <DF>;
my $goodies;
chomp ($goodies = <DF>);
my ($fs,$size,$used,$avail,$useperc,$trash) = split(/\s+/,$goodies);
# insertusage($fs,$size,$used,$avail,$useperc);
close DF;

# Finally, collecting the missing blocks and live nodes, I recommend starting here as it is very straightforward and is very useful data to know.

open(REPORT,"/usr/bin/hdfs dfsadmin -report|") || die "Could not run $!\n";
my $missing;
my $nodes;
while (<REPORT>) {
    chomp;
        if ( /Missing blocks:\s(\d+)/ ) {
                $missing = $1;
                    }
                        if ( /Live datanodes\s\((\d+)\):/ ) {
                                $nodes = $1;
                                    }                                   }
