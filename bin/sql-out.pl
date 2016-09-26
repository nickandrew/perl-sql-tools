#!/usr/bin/perl
#   @(#) sql-out.pl - Dump a table contents to stdout
#
# Usage: sql-out.pl tablename fieldname ... > output
#
# Output file format:
#   fieldname|fieldname|fieldname ...
#   value|value|value ...
#   value|value|value ...
#   value|value|value ...
#

use strict;
use warnings;

use DBI qw();

my $driver = 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = 'nick';
my $password = '';
my $options = '';

if ($host) {
	$options = "host=$host;" . $options;
}

my $database = shift @ARGV || die "Database name required in argv";

my $table = shift @ARGV;
if ($table eq '') {
	print STDERR "Usage: sql-out.pl tablename fieldname ... > output\n";
	exit(4);
}

my @fieldlist = @ARGV;
if (!@fieldlist) {
	print STDERR "No field list supplied!\n";
	exit(4);
}

print join('|', @fieldlist), "\n";

my $dsn = "DBI:$driver:database=$database;$options";
my $dbh = DBI->connect($dsn, $user, $password);

if (!defined $dbh) {
	print "Unable to connect to database, sorry!\n";
	exit(3);
}

my $sql = "SELECT " . join(',', @fieldlist) . " FROM $table";

$dbh->do("LOCK TABLES $table READ");

my $sth = $dbh->prepare($sql);
if (!defined $sth) {
	print "Unable to prepare sql: ", $dbh->errstr(), "\n";
	exit(4);
}

$sth->execute();

my @row;

while (@row = $sth->fetchrow_array()) {
	print join('|', @row), "\n";
}
$sth->finish();

$dbh->do("UNLOCK TABLES");

my $rv = $dbh->disconnect();
if ($rv != 1) {
	print "Error on disconnect: $rv\n";
	exit(6);
}

exit(0);
