#!/usr/bin/perl
#	@(#) sql-export.pl - Dump the results of a select statement to stdout
#
# Usage: sql-export.pl [-d database] 'statement' > output
#
# Output file format:
#	fieldname|fieldname|fieldname ...
#	value|value|value ...
#	value|value|value ...
#	value|value|value ...
#

use DBI qw();
use Getopt::Std qw(getopts);

use vars qw($opt_d $opt_l $opt_t);

my $driver = $ENV{DB_DRIVER} || 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = $ENV{DB_USER} || '';
my $password = $ENV{DB_PASSWORD} || '';
my $options = $ENV{DB_OPTIONS} || '';

if ($host) {
	$options = "host=$host;" . $options;
}

getopts('d:l:t');

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

my $row_hr;
my @keys;
my @fieldlist;

while (defined ($row_hr = $sth->fetchrow_hashref())) {

	if (!@fieldlist) {

		# Grab the field names

		foreach my $k (sort (keys %$row_hr)) {
			push(@fieldlist, lc($k));
			push(@keys, $k);
		}

		print join('|', @fieldlist), "\n";
	}

	# Output the row

	foreach my $k (@keys) {
		if ($row_hr->{$k} =~ /\||\n/) {
			die "Pipe or newline in data field $k";
		}
	}

	print join('|', (map { $row_hr->{$_} } @keys)), "\n";

	if ($opt_l > 0) {
		$opt_l--;
		last if ($opt_l == 0);
	}
}

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
	die "Usage: sql-export.pl database 'statement' > output\n";
}

sub print_pipe {
	if (!@fieldlist) {

		# Grab the field names

		foreach my $k (sort (keys %$row_hr)) {
			push(@fieldlist, lc($k));
			push(@keys, $k);
		}

		print join('|', @fieldlist), "\n";
	}

	# Output the row

	foreach my $k (@keys) {
		if ($row_hr->{$k} =~ /\||\n/) {
			die "Pipe or newline in data field $k";
		}
	}

	print join('|', (map { $row_hr->{$_} } @keys)), "\n";
}

sub print_para {
	foreach my $k (sort (keys %$row_hr)) {
		my $v = $row_hr->{$k};
		if ($v =~ /\n/) {
			die "newline in data field $k";
		}

		printf "%s: %s\n", $k, $v;
	}

	print "\n";
}
