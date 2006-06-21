use Test::More tests => 39;
use Cwd;
use File::Path qw(rmtree);

use_ok('Cache::BDB');
use Cache::BDB;

my $cache_root_base = './t/01';

END {
  rmtree($cache_root_base);
}

# verify that we can create a cache with no explicit file name and that its 
# db file will web named $namespace.db

my $c = Cache::BDB->new(cache_root => $cache_root_base,
			namespace => 'test',
			default_expires_in => 10,
			type => 'Btree');

ok(-e join('/', 
	   $cache_root_base,
	   'test.db'));

# verify that we'll create a full path if need be
my $f = Cache::BDB->new(cache_root => join('/',
					   $cache_root_base,
					   'Cache::BDB',
					   $$,
					   'test'),
			namespace => 'whatever');
ok(-e join('/', 
	   $cache_root_base,
	   'Cache::BDB',
	   $$,
	   'test',
	   'whatever.db'));


# verify that we can create a single file with multiple dbs
my @names = qw(one two three four five six);

for my $name (@names) {
    my %options = (
	cache_root => $cache_root_base,
	cache_file => "one.db",
	namespace => $name,
	default_expires_in => 10,
    );	

    #diag("opening one.db with namespace $name");
    my $c = Cache::BDB->new(%options);
    isa_ok($c, 'Cache::BDB');
    is($c->set(1, $name),1);
    is($c->count(), 1);
    undef $c;
}

# verify that those databases can be connected to and contain what we
# put in them

for my $name (@names) {
    my %options = (
	cache_root => './t/01',
	cache_file => "one.db",
	namespace => $name,
	default_expires_in => 10,
    );	
    
    my $c = Cache::BDB->new(%options);
    isa_ok($c, 'Cache::BDB');
    is($c->get(1), $name);
    is($c->count(), 1);
    undef $c;
}


