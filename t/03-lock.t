use Time::HiRes qw(tv_interval gettimeofday);
use Test::More qw(no_plan);
use Data::Dumper;
use Cache::BDB;
use strict;

my $kids = 7; # number of children to spawn
my $iterations = 2; # number of times each kid should do its thing
my $rows = 60; # number of rows each child should write, then read

my %options = (
	cache_root => './t/03',
	cache_file => "one.db",
	namespace => "Cache::BDB::lock",
	default_expires_in => 10,
);	

# create a cache object so the environment is already in place, but then undef
# it so we don't give each child multiple handles
my $c = Cache::BDB->new(%options);
$c->clear();
undef $c;

my @pids = ();
for(my $i = 0; $i <= $kids; $i++) {
    if(my $pid = fork() ) {
	push @pids, $pid;
    } else {
	run_child();
    }
}

diag("spawned $kids children " . join(', ', @pids));

foreach my $kid (@pids) {
    waitpid($kid, 0);
    diag("$kid done");
}

$c = Cache::BDB->new(%options);
diag("found " . $c->count() . " records");
is($c->count(), $rows);

sub run_child {
	    
    my $t0 = [gettimeofday];
    
    my %results;
    my $c = Cache::BDB->new(%options);

    my @ids;
    for my $it (0 .. $iterations) {
      for (my $j = 1; $j <= $rows; $j++) {
	my $r = ($j ** $it)  x 4;
	
	my $rv = $c->set($j, {$j => $r} );
	push @ids, $j;

      }
    }

    for(0 .. $iterations) {
      for(@ids) {
	
	my $rv = $c->get($_);
	$results{$$}->{$_} = $rv;
      }
    }

    my $t1 = [gettimeofday];
    diag("$$: finished in " . tv_interval($t0, $t1) .  " seconds");
#    diag(Dumper \%results);
    exit;
}
