#!/usr/bin/perl
#	@(#) sql-export.pl - Dump the results of a select statement to stdout
#
# Usage: sql-do.pl [-d database] 'statement'
#

use DBI qw();
use Getopt::Std qw(getopts);

use vars qw($opt_d $opt_l $opt_p $opt_t);

my $driver = $ENV{DB_DRIVER} || 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = $ENV{DB_USER} || '';
my $password = $ENV{DB_PASSWORD} || '';
my $options = $ENV{DB_OPTIONS} || '';

if ($host) {
	$options = "host=$host;" . $options;
}

getopts('d:l:pt');

my $database = $opt_d || $ENV{DB_DATABASE};
my $statement = shift @ARGV || usage();

my $dsn = $ENV{DB_DSN} || "DBI:$driver:database=$database;$options";
my $dbh = DBI->connect($dsn, $user, $password);

if (!defined $dbh) {
	print "Unable to connect to database, sorry!\n";
	exit(3);
}

$dbh->{LongReadLen} = 16384;

if ($opt_t) {
	$dbh->do("LOCK TABLES $opt_t READ");
}

# print "Statement is <$statement>\n";

my $sth = $dbh->prepare($statement);
if (!defined $sth) {
	print "Unable to prepare sql: ", $dbh->errstr(), "\n";
	exit(4);
}

$sth->execute();

$sth->finish();

if ($opt_t) {
	$dbh->do("UNLOCK TABLES");
}

my $rv = $dbh->disconnect();
if ($rv != 1) {
	print "Error on disconnect: $rv\n";
	exit(6);
}

exit(0);

sub usage {
	die "Usage: sql-do.pl -d database 'statement' > output\n";
}
