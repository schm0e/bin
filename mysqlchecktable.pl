#!/usr/bin/perl
$|++;

use Getopt::Std;
use DBI;

my %options = ();

getopts("rvD:h:p:u:",\%options);
# r & v are boolean flag (no ':' afterwards)
# D,h,p,u,v take args (followed by ':')

usage() if ! $options{'D'};
usage() if ! $options{'h'};
usage() if ! $options{'u'};
usage() if ! $options{'p'};

my $datadir = '/var/lib/mysql';

my $dbh = DBI->connect(
    "DBI:mysql:database=$options{'D'};host=$options{'h'}",$options{'u'},$options{'p'},
    { RaiseError => 1, PrintError => 1 }
    ) || die $DBI::errstr;

my $tables = $dbh->selectall_hashref(
    qq~select `TABLE_NAME` from `INFORMATION_SCHEMA`.`tables` where `TABLE_SCHEMA`='$options{'D'}'~,
    'TABLE_NAME'
    ) or warn $dbh->errstr;

my $errorflag;
my %errortables;

if(! $options{'v'} ) {
    print `uptime`;
}
for my $tablename( keys %$tables ) {
    my $stmt = qq~check table $options{'D'}.$tablename~;
    my $check = $dbh->selectall_arrayref($stmt);
    if( $check->[$#$check]->[3] !~ /OK/i || ! $options{'v'} ) {
        printf '%-50s',"/* $stmt; */";
        print ' ' . $check->[$#$check]->[2] . ' ' . $check->[$#$check]->[3] . "\n";
        if( $check->[$#$check]->[3] !~ /OK/i ) {
            $errorflag++;
            $errortables{$tablename}++;
        }
    }
}
if( $errorflag ) {
    if( $options{r} ) {
        if( -d "$datadir/$options{D}" ) {
            chdir "$datadir/$options{D}" || die $!;
            print "#--- `service crond stop`\n";
            print `service crond stop`;
            print "#--- `service mysqld stop`\n";
            print `service mysqld stop`;
            for my $tablename ( sort keys %errortables ) {
                if ( -f "$datadir/$options{D}/$tablename.MYI" ) {
                    print "#--- `myisamchk -r $tablename.MYI`\n";
                    print `myisamchk -r $tablename.MYI`;
                } else {
                    print "#--- can't restore. file '$datadir/$options{D}/$tabelname.MYI' not found\n";
                }
            }
            print "#--- `service mysqld start`\n";
            print `service mysqld start`;
            print "#--- `service crond start`\n";
            print `service crond start`;
        }
    } else {
        print <<"thIs";
tables need to be repaired:
  [root\@host]# cd /var/lib/mysql/$options{D}
  [root\@host]# service mysqld stop
  [root\@host]# myisamchk -r *MYI
  [root\@host]# service mysqld start
thIs
    }
}
sub usage {
    print "Usage:\n";
    print <<"thIs";
    $0 [-rv] -h localhost -D dbname -u dbuser -p dbsecret
        -r (optional boolean flag default is false
            to not to try myisamchk recovery)
        -v (optional boolean flag default is false
            to show all tables, true shows only the tables with
            errors)
        -h hostname
        -D databasename
        -u username
        -p passwd
thIs
    exit 1;
}
