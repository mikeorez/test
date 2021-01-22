#!/usr/bin/perl -w

use strict;
use warnings;

use Config::Tiny;
use DBI;
use Getopt::Long;
use Mail::Exim::MainLogParser;
use Pod::Usage;

my %opt = ();
GetOptions (\%opt,
    'config|conf|cfg=s',
    'file|f=s',
    'v|verbose',
    'bucket_size=i',
    'help|h|?',
) or pod2usage(2);
pod2usage(1) if $opt{'help'};

unless (exists $opt{'file'}) {
    if ($ARGV[0] && -f $ARGV[0]) {
        $opt{'file'} = $ARGV[0];
    } else {
        pod2usage(2);
    }
}

unless ($opt{'config'}) {
    $opt{'config'} = 'test.cfg' if -f 'test.cfg';
}
die "Try to use the -config parameter\n" unless $opt{'config'};

my $cfg = Config::Tiny->read($opt{'config'}) or die sprintf 'Config file (%s) read error: %s', $opt{'config'}, Config::Tiny->errstr();

my %bounce = ();
my %stat = ();

$opt{'bucket_size'} ||= 100;
my $records_in_transaction = 0;

my $dbh = DBI->connect(
    "dbi:Pg:dbname=$cfg->{db}->{dbname};host=$cfg->{db}->{host};port=$cfg->{db}->{port}",
    $cfg->{db}->{user},
    $cfg->{db}->{password},
    { AutoCommit => 1 }
) or die 'Cannot connect to DB: ' . DBI->errstr;

open my $fh, '<', $opt{'file'} or die sprintf 'Cannot open file %s: %s', $opt{'file'}, $!;

my $sth_msg = $dbh->prepare('INSERT INTO message (created, id, int_id, str) VALUES (?,?,?,?)') or die 'Cannot prepare sth_msg: ' . $dbh->errstr;
my $sth_log = $dbh->prepare('INSERT INTO log (created, int_id, str, address) VALUES (?,?,?,?)') or die 'Cannot prepare sth_log: ' . $dbh->errstr;
$dbh->begin_work;

my $exlog = new Mail::Exim::MainLogParser;

while (<$fh>) {
    chomp;
    my $rec = $exlog->parse($_);
    $stat{'lines parsed'}++;

    my $str = substr($_, 20);
    my $created = $rec->{'date'} . 'T' . $rec->{'time'};

    # Skip messages without int_id
    next unless $rec->{'eximid'};

    # Skip bounce messages
    if (exists $bounce{$rec->{'eximid'}}) {
        delete $bounce{$rec->{'eximid'}} if exists($rec->{'message'}) && $rec->{'message'} eq 'Completed';
        next;
    }

    if (exists($rec->{'flag'}) && $rec->{'flag'} eq '<=') {
        if ($rec->{'address'} eq '<>') {
            ++$bounce{$rec->{'eximid'}};
            warn "Bounce message skipped: " . $_ . "\n" if $opt{'v'};
            next;
        }

        my $id = '';
        for my $arg (@{$rec->{'args'}}) {
            if (exists $arg->{'id'}) {
                $id = $arg->{'id'};
                last;
            }
        }
        unless ($id) {
            warn sprintf "id field not found: %s %s %s\n", $rec->{'date'}, $rec->{'time'}, $rec->{'eximid'};
            next;
        }

        $sth_msg->execute($created, $id, $rec->{'eximid'}, $str) or die 'Cannot execute sth_msg: ' . $sth_msg->errstr;
        $stat{'msg records'}++;
    } else {
        $rec->{'address'} //= '';
        my $address = $rec->{'address'} =~ /<(\S+)>/
            ? $1 
            : $rec->{'address'};

        $sth_log->execute($created, $rec->{'eximid'}, $str, $address) or die 'Cannot execute sth_msg: ' . $sth_log->errstr;
        $stat{'log records'}++;
    }

    if (++$records_in_transaction >= $opt{'bucket_size'}) {
        $dbh->commit;
        $dbh->begin_work;
        $records_in_transaction = 0;
    }
}
close $fh;

$dbh->commit;

print map { "$_:\t$stat{$_}\n" } sort keys %stat;

$sth_msg->finish;
$sth_log->finish;
$dbh->disconnect;

__END__
 
=head1 NAME
 
parser - The mail log parser
 
=head1 SYNOPSIS
 
parser [options]
 
 Options:
   -file            input file name (mandatory)
   -config          configuration file name
   -help            brief help message
 
=head1 OPTIONS
 
=over 8
 
=item B<-help>
 
Print a brief help message and exits.
 
=item B<-config>

Path to a custom config file. Default is the same as script name with '.cfg' extension

=back
 
=head1 DESCRIPTION
 
B<This program> will read the given input file(s) and put the contents to database
 
=cut
