use Test::More tests => 38;
use Cwd;

use_ok('Cache::BDB');
use Cache::BDB;

my %options = (
    cache_root => './t/01',
    namespace => 'test',
    default_expires_in => 10,
    type => 'Btree',
);	

END {
   unlink(join('/', 
	       $options{cache_root},
	       $options{namespace}));
}

# verify that we can create a cache with no explicit file name and that its 
# db file will web named $namespace.db

my $c = Cache::BDB->new(%options);
ok(-e join('/', 
	   $options{cache_root},
	   $options{namespace} . ".db"));


# verify that we can create a single file with multiple dbs
my @names = qw(one two three four five six);

for my $name (@names) {
    my %options = (
	cache_root => './t/01',
	cache_file => "one.db",
	namespace => $name,
	default_expires_in => 10,
    );	
    
    my $c = Cache::BDB->new(%options);
    isa_ok($c, 'Cache::BDB');
    is($c->set(1, 'foo'),1);
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
    is($c->get(1), 'foo');
    is($c->count(), 1);
    undef $c;
}


