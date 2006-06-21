use Test::More tests => 52;
use Data::Dumper;
use Cache::BDB;

my %options = (
	cache_root => './t/02',
	cache_file => "02-api.db",
	namespace => "Cache::BDB::02",
	default_expires_in => 10,
);	

END {
   unlink(join('/', 
	       $options{cache_root},
	       $options{cache_file}));
}

my $hash1 = {foo=>'bar'};
my $hash2 = {bleh => 'blah', doof => [4,6,9]};
my $array1 = [1, 'two', 3];
my $array2 = [3,12,123,213,213213,4354356,565465,'das1', 'two', 3];
my $obj1 = bless ( {foo => $hash1, bleh => $hash2, moobie => $array2},  'Some::Class');
my $c = Cache::BDB->new(%options);

ok(-e join('/', 
	   $options{cache_root},
	   $options{cache_file}));

isa_ok($c, 'Cache::BDB');
can_ok($c, qw(set get remove purge size count namespace));

is($c->set(1, $hash1), 1);
is_deeply($c->get(1), $hash1);
is($c->count, 1);

is($c->set(2, $hash2),1);
is_deeply($c->get(2), $hash2);
is($c->count, 2);

is($c->set(3, $array1),1);
is_deeply($c->get(3), $array1);
is($c->count, 3);

is($c->set(4, $obj1),1);
is_deeply($c->get(4), $obj1);
is($c->count, 4);

is($c->remove(1), 1);
is($c->get(1),undef);
is($c->count, 3);

is($c->set(5, $array2,2),1);
is($c->count, 4);

is($c->set(6, $hash1,20),1);
is($c->count, 5);

sleep 3;

is($c->is_expired(5), 1, "expired? (should be)");
is($c->purge(), 1);

is($c->is_expired(6), 0, "expired? (shouldn't be)");
is($c->get(5),undef);

is($c->count, 4);
is_deeply($c->get(6),$hash1);

is($c->clear(), 4);
is($c->get(2),undef);
is($c->get(3),undef);

is($c->count, 0);

is($c->set(7, $hash1),1);
is($c->set(8, $hash2),1);
is($c->set(9, $array1),1);
is($c->set(10, $array2),1);

is($c->count, 4);

is($c->set(10, $hash2), 1);

is_deeply($c->get(10), $hash2);

undef $c;
is(undef, $c);
my $c2 = Cache::BDB->new(%options);

is_deeply($c2->get(7), $hash1);
is_deeply($c2->get(8), $hash2);
is_deeply($c2->get(9), $array1);
is_deeply($c2->get(10), $hash2);

is($c2->set('foo', 'bar'),1);
is($c2->get('foo'), 'bar');

my %h = (some => 'data', goes => 'here');
is($c2->set(100, \%h), 1);

is_deeply(\%h, $c2->get(100));

is($c2->add(100, \%h), 0, "Can't add, already exists");
is($c2->replace(100, \%h), 1, 'Can replace, already exists');

is($c2->add(101, \%h), 1, "Can add, doesn't exist yet");
is($c2->replace(102, \%h), 0, "Can't replace, doesn't exist");
