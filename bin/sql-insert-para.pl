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

use SQL qw();
use Getopt::Std qw(getopts);

use vars qw($opt_c $opt_d $opt_t);

my $driver = $ENV{DB_DRIVER} || 'mysql';
my $host = $ENV{DB_HOST} || '';
my $user = $ENV{DB_USER} || '';
my $password = $ENV{DB_PASSWORD} || '';
my $options = $ENV{DB_OPTIONS} || '';

if ($host) {
	$options = "host=$host;" . $options;
}

getopts('cd:t');

my $database = $opt_d || $ENV{DB_DATABASE};

my $dsn = $ENV{DB_DSN} || "DBI:$driver:database=$database;$options";
my $sql = new SQL();


my $table = $ARGV[0];
if ($table eq '') {
	print "Usage: sqlinsert.pl [-d database] tablename <inputfile\n";
	exit(2);
}

my %data;

while (<STDIN>) {
	chomp;

	if (/^$/) {
		if (%data) {
			insert_row($sql, \%data);
			undef %data;
		}
		next;
	}

	if (/^([^:]+): (.*)/) {
		$data{$1} = $2;
	}
}

if (%data) {
	insert_row($sql, \%data);
}

if ($opt_c) {
	print "Committing\n";
	$sql->Commit();
} else {
	print "Not committing (use -c option to commit)\n";
}

exit(0);

sub insert_row {
	my $sql = shift;
	my $hr = shift;

	my $new_oid = $sql->get_unique('oid');

	if (exists $hr->{oid}) {
		print STDERR "Warning: ignoring oid $hr->{oid} (replaced with $new_oid)\n";
	} 

	$hr->{oid} = $new_oid;

	my @fieldlist = sort(keys %$hr);

	my $stmt = "INSERT into $table (" .
		join(',', @fieldlist) .
		') VALUES (' .
		join(',', (map { '?' } @fieldlist)) .
		')';

	print "STMT is $stmt\n";
	print "Values are ", join(',', (map { $hr->{$_} } @fieldlist)), "\n";

	$sql->Execute($stmt, map { $hr->{$_} } @fieldlist);
}
