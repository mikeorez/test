#!/usr/bin/perl -w

use strict;
use warnings;

use Config::Tiny;
use DBI;
use Getopt::Long;
use HTML::Template;
use HTTP::Server::PSGI;
use Log::Any '$log';
use Plack::Request;
use Pod::Usage;
use POSIX;

my %opt = ();

GetOptions (\%opt,
    'config|conf|cfg=s',
    'loglevel=s',
    'host=s',
    'port=s',
    'timeout=i',
    'help|h|?',
) or pod2usage(2);
pod2usage(1) if $opt{'help'};

unless ($opt{'config'}) {
    $opt{'config'} = 'test.cfg' if -f 'test.cfg';
}
die "Try to use the -config parameter\n" unless $opt{'config'};

my $cfg = Config::Tiny->read($opt{'config'}) or die sprintf 'Config file (%s) read error: %s', $opt{'config'}, Config::Tiny->errstr();

use Log::Any::Adapter;
Log::Any::Adapter->set('Screen',
    min_level => $opt{'loglevel'} || 'debug',
    use_color => 0,
    stderr    => 0,
    formatter => sub { strftime('%Y-%m-%d %H:%M:%S' , localtime) . " [$$] $_[1]" },
);

my $server = HTTP::Server::PSGI->new(
    host => $opt{'host'} || $cfg->{'srv'}->{'host'} || '127.0.0.1',
    port => $opt{'port'} || $cfg->{'srv'}->{'port'} || 80,
    timeout => $opt{'timeout'} || $cfg->{'srv'}->{'timeout'} || 120,
);

my $dbh = DBI->connect(
    "dbi:Pg:dbname=$cfg->{db}->{dbname};host=$cfg->{db}->{host};port=$cfg->{db}->{port}",
    $cfg->{db}->{user},
    $cfg->{db}->{password},
    { AutoCommit => 1 }
) or die 'Cannot connect to DB: ' . DBI->errstr;

my $sth = $dbh->prepare(
    'WITH iii AS (SELECT DISTINCT int_id FROM log WHERE address=? order by int_id limit ?) '.
    'SELECT created,str FROM ('.
        'SELECT m.created,m.int_id,m.str FROM iii,message m WHERE m.int_id IN (iii.int_id) '.
        'UNION ALL '.
        'SELECT l.created,l.int_id,l.str FROM iii,log l WHERE l.int_id IN (iii.int_id)'.
    ') aaa ORDER BY int_id,created LIMIT ?'
)or die 'Cannot prepare sth: ' . $dbh->errstr;

my $template = HTML::Template->new(filename => 'response.tmpl');

my $app = sub {
    STDERR->binmode(':encoding(UTF-8)');
    my $env = shift;
    $log->tracef('New request, env: %s', $env);
    my $req = Plack::Request->new($env);

    $log->infof('%s "%s %s"', $env->{'REMOTE_ADDR'}, $req->method, $req->request_uri);
    unless ($req->path =~ m!^\/test$!) {
        $log->infof('Path check failed: "%s"', $req->path);
        return [ '404', [], ];
    }

    my $addr = $req->query_parameters->get('addr') || '';

    if ($addr) {
        $template->param(ADDR => $addr);
        $template->param(MORE_RESULTS => 0);
        if ($addr =~ /[^-_.!\@a-zA-Z0-9]/) {
            $log->warnf('Bad address: "%s"', $addr);
        } else {
            my $limit = $cfg->{'srv'}->{'res_limit'} || 100;
            $sth->execute($addr, $limit+1, $limit+1) or $log->errorf('sth execute failed: %s',$sth->errstr) and return [500, [],['DB error']];

            my $rows = $sth->fetchall_arrayref({});
            if (scalar @$rows > $limit) {
                $template->param(MORE_RESULTS => 1);
                pop @$rows;
            }
            $template->param(RESULT => $rows);
        }
    }

    my $res = $req->new_response(200);
    $res->header('Content-Type' => 'text/html', charset => 'utf-8');
    my $body = $template->output;

    utf8::encode $body;
    $res->body($body);

    return $res->finalize();
};

$server->run($app);

__END__
 
=head1 NAME
 
srv - The simple HTTP server
 
=head1 SYNOPSIS
 
srv [options]
 
 Options:
   -config          configuration file name
   -host            server IP
   -port            server port
   -loglevel        log level (trace, debug, info, notice, warning, error, critical, alert, emergency)
   -help            brief help message
 
=head1 OPTIONS
 
=over 8
 
=item B<-help>
 
Print a brief help message and exits.
 
=item B<-config>

Path to a custom config file. Default is the same as script name with '.cfg' extension

=back
 
=head1 DESCRIPTION
 
B<This program> is a Web-interface to the test database
 
=cut
