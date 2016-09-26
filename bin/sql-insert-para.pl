#!/usr/bin/perl
#   @(#) sql-insert-para.pl - Control-file-driven bulk insertion
#
# Usage: sql-insert-para.pl tablename < inputfile
#
# Input file format:
#   fieldname: value
#   fieldname: value
#   etc... empty line to end the row.

use strict;
use warnings;

use DBI qw();
use Getopt::Std qw(getopts);

use vars qw($opt_c $opt_d $opt_e $opt_l $opt_t $opt_z);

my $driver = $ENV{DB_DRIVER} || 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = $ENV{DB_USER} || '';
my $password = $ENV{DB_PASSWORD} || '';
my $options = $ENV{DB_OPTIONS} || '';

if ($host) {
	$options = "host=$host;" . $options;
}

getopts('cd:e:ltz');

my $database = $opt_d || $ENV{DB_DATABASE};
$opt_t || usage();
my $max_errors = $opt_e || 0;

my $dsn = $ENV{DB_DSN} || "DBI:$driver:database=$database;$options";
my $dbh = DBI->connect($dsn, $user, $password);

if (!defined $dbh) {
	print "Unable to connect to database, sorry!\n";
	exit(3);
}

my $table = $ARGV[0];
if ($table eq '') {
	print "Usage: sql-insert-para.pl [-d database] [-lz] tablename <inputfile\n";
	exit(2);
}

if ($opt_l) {
	$dbh->do("LOCK TABLES $opt_t WRITE");
}

# ---------------------------------------------------------------------------
# Optionally delete everything from the table
# ---------------------------------------------------------------------------

if ($opt_z) {
	my $sth = $dbh->prepare("DELETE from $opt_t");
	$sth->execute();
}

my %data;
my $rc = 0;
my $row_count = 0;
my $errors = 0;

while (<STDIN>) {
	chomp;

	if (/^$/) {
		if (%data) {
			insert_row($dbh, \%data);
			undef %data;
		}
		next;
	}

	if (/^([^:]+): (.*)/) {
		$data{$1} = $2;
	}
}

if (%data) {
	insert_row($dbh, \%data);
}

if ($opt_c) {
	print "Committing\n";
	$dbh->commit();
} else {
	print "Not committing (use -c option to commit)\n";
}

if ($opt_l) {
	$dbh->do("UNLOCK TABLES");
}

my $rv = $dbh->disconnect();
if ($rv != 1) {
	print "Error on disconnect: $rv\n";
	exit(6);
}

exit($rc);

sub usage {
	print STDERR "Usage: sql-insert-para.pl [-d database] [-e max_errors] [-l] [-t table_name] [-z] <inputfile\n";
	print STDERR <<EOF;
-l	Lock Tables
-z	Zero table (delete everything before insert)
EOF

	exit(8);
}

sub insert_row {
	my $dbh = shift;
	my $hr = shift;

	my @fieldlist = sort(keys %$hr);

	my $stmt = "INSERT into $table (" .
		join(',', @fieldlist) .
		') VALUES (' .
		join(',', (map { '?' } @fieldlist)) .
		')';

	# print "STMT is $stmt\n";
	# print "Values are ", join(',', (map { $hr->{$_} } @fieldlist)), "\n";

	my $sth = $dbh->prepare($stmt);
	my $rv = $sth->execute(map { $hr->{$_} } @fieldlist);
	if ($rv != 1) {
		print "Error on insert ($row_count): ", $sth->errstr(), "\n";
		$rc = 8;
		$errors++;
		if ($max_errors && $errors > $max_errors) {
			print " ... too many errors, exiting\n";
			last;
		}
	}

	$row_count ++;
}
