#!/usr/bin/perl
#   @(#) sql-update.pl - Control-file-driven bulk row update
#
# Usage: sql-update.pl tablename keyfield < inputfile
#
# Input file format:
#   fieldname|fieldname|fieldname ...
#   value|value|value ...
#   value|value|value ...
#   value|value|value ...

use strict;
use warnings;

use DBI qw();
use Getopt::Std qw(getopts);

use vars qw($opt_c $opt_d $opt_s $opt_t);

my $driver = $ENV{DB_DRIVER} || 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = $ENV{DB_USER} || '';
my $password = $ENV{DB_PASSWORD} || '';
my $options = $ENV{DB_OPTIONS} || '';

if ($host) {
	$options = "host=$host;" . $options;
}

getopts('cd:s:t');

my $database = $opt_d || $ENV{DB_DATABASE};

my $dsn = $ENV{DB_DSN} || "DBI:$driver:database=$database;$options";
my $dbh = DBI->connect($dsn, $user, $password);

if (!defined $dbh) {
	print "Unable to connect to database, sorry!\n";
	exit(3);
}

$dbh->{LongReadLen} = 16384;
$dbh->{AutoCommit} = 0;

if ($opt_s) {
	$dbh->do("SET SCHEMA $opt_s");
}


my $table = shift @ARGV || usage();
my $keyfield = shift @ARGV || usage();


my $fields;
chop($fields = <STDIN>);
my @fieldlist = split(/\|/, $fields);

my @fields;
my $key_found = 0;
foreach (@fieldlist) {
	if ($_ eq $keyfield) {
		$key_found = 1;
	} else {
		push(@fields, $_);
	}
}

if (! $key_found) {
	die "Keyfield $keyfield not in input file";
}

if ($opt_t) {
	$dbh->do("LOCK TABLES $table WRITE");
}

my $sql = "UPDATE $table SET " .
	join(',',
		map { "$_ = ?" } (@fields)
	)
	. " WHERE $keyfield = ?"
;

print "SQL is $sql\n";

my $sth = $dbh->prepare($sql);
if (!defined $sth) {
	print "Unable to prepare sql: ", $dbh->errstr(), "\n";
	exit(4);
}

my @data;
my @bound;
my $rv;
my $rc = 0;
my $v;
my $rows = 0;

while (<STDIN>) {
	chop;
	my $line = $_;
	@data = split(/\|/);
	@bound = ();
	my $key;

	foreach (@fieldlist) {
		if ($_ eq $keyfield) {
			$key = shift @data;
		} else {
			$v = shift @data;
			$v = undef if ($v eq '');
			push(@bound, $v);
		}
	}
	$rv = $sth->execute(@bound, $key);
	if ($rv != 1) {
		print "Error on update ($line): ", $sth->errstr(), "\n";
		print "Bound was (", join(',', @bound), ") and key was ($key)\n";
		$rc = 8;
	}

	# Unlock and relock tables every 1000 rows for interactive performance
	if ($opt_t && (++$rows % 1000) == 0) {
		$dbh->do("UNLOCK TABLES");
		$dbh->do("LOCK TABLES $table WRITE");
	}
}

if ($opt_t) {
	$dbh->do("UNLOCK TABLES");
}

$sth->finish();

if ($opt_c) {
	$dbh->commit();
	print "Committed\n";
} else {
	$dbh->rollback();
	print "Rolled Back (use -c next time)\n";
}

$rv = $dbh->disconnect();
if ($rv != 1) {
	print "Error on disconnect: $rv\n";
	exit(6);
}

exit($rc);

sub usage {
	die "Usage: sql-update.pl [-d database] tablename keyfield <inputfile\n";
}
