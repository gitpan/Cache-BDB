package Cache::BDB;

# $Id: BDB.pm,v 1.3 2005/12/13 04:25:14 jrotenberg Exp $

use strict;
use warnings;

use BerkeleyDB;
use Storable;
use Data::Dumper;

our $VERSION = '0.01';

use constant DEFAULT_DB_TYPE => 'Hash';
my $__meta_version = '001';

=pod

=head1 NAME

Cache::BDB - An object caching wrapper around BerkeleyDB

=head1 SYNOPSIS

 use Cache::BDB;
 my %options = (
    cache_root => "/tmp",
    namespace => "Some::Namespace",
    default_expires_in => 300, # seconds
 );

 my $cache = Cache::BDB->new(%options);

 $cache->namespace(); # returns "Some::Namespace", read only
 $cache->default_expires_in(); # returns 300
 $cache->default_expires_in(600); # change it to 600

 $cache->set(1, \%some_hash);
 $cache->set('foo', 'bar');
 $cache->set(20, $obj, 10);

 my $h = $cache->get(1); # $h and \%some_hash contain the same data
 my $bar = $cache->get('foo'); # $bar eq 'bar';
 my $obj = $cache->get(20); # returns the blessed object

 $cache->count() == 3;
 # assuming 10 seconds has passed ...
 $cache->is_expired(20); # returns true ..
 $cache->purge();
 $cache->get(20); # returns undef
 $cache->count() == 2;

 undef $cache; # close the cache object

=head1 DESCRIPTION

This module implements a caching layer around BerkeleyDB
for object persistence. It implements the basic methods necessary to
add, retrieve, and remove objects. The main advantage over other
caching modules is performance. I've attempted to stick with a
B<Cache::Cache> like interface as much as possible.

=head1 PERFORMANCE

The intent of this module is to supply great performance with a
reasonably feature rich API. There is no way this module can compete
with, say, using BerkeleyDB directly, but if you don't need any kind
of expiration, automatic purging, etc, that will more than likely be
much faster. If you'd like to compare the speed of some other caching
modules, have a look at
B<http://cpan.robm.fastmail.fm/cache_perf.html>.  I've included a
patch which adds Cache::BDB to the benchmark.

=head1 LOCKING

All cache Berkeley DB environments are opened with the DB_INIT_CDB
flag. This enables multiple-reader/single-writer locking handled
entirely by the Berkeley DB internals at either the database or
environment level. See
http://www.sleepycat.com/docs/ref/cam/intro.html for more information
on what this means for locking.

=head1 CACHE FILES

For every new B<Cache::BDB> object, a Berkeley DB Environment is
created (or reused if it already exists). This means that even for a
single cache object, at least 4 files need to be created, three for
the environment and at least one for the actual data in the cache. Its
possible for mutliple cache database files to share a single
environment, and its also possible for multiple cache databases to
share a single database file.

=head1 USAGE

=over 4

=item B<new>(%options)

=item * cache_root

Specify the top level directory to store cache and related files
in. This parameter is required. Keep in mind that B<Cache::BDB> uses a
B<BerkeleyDB> environment object so more than one file will be written
for each cache.

=item * cache_file

If you want to tell B<Cache::BDB> exactly which file to use for your
cache, specify it here. This paramater is required if you plan to use
the env_lock option and/or if you want to have multiple databases in
single file. If unspecified, B<Cach::BDB> will create its database file using
the B<namespace>.

=item * namespace

Your B<namespace> tells B<Cache::BDB> where to store cache data under
the B<cache_root> if no B<cache_file> is specified or what to call the
database in the multi-database file if B<cache_file> is specified. It
is a required parameter.

=item * env_lock

If multiple databases (same or different files) are opened using the
same Berkeley DB environment, its possible to turn on environment
level locking rather than file level locking. This may be advantageous
if you have two separate but related caches. By passing in the
env_lock parameter with any true value, the environment will be
created in such a way that any databases created under its control
will all lock whenever Berkeley DB attempts a read/write lock. This
flag must be specified for every database opened under this
environment.

=item * default_expires_in

Time (in seconds) that cached objects should live. If set to 0,
objects never expire. See B<set> to enable a per-object value.

=item * auto_purge_interval

Time (in seconds) that the cached will be purged by one or both of the
B<auto_purge> types (get/set). If set to 0, auto purge is disabled.

=item * auto_purge_on_set

If this item is true and B<auto_purge_interval> is greater than 0,
calling the B<set> method will first purge any expired records from
the cache.

=item * auto_purge_on_get

If this item is true and B<auto_purge_interval> is greater than 0,
calling the B<get> method will first purge any expired records from
the cache.

=item * purge_on_init

If set to a true value, purge will be called before the constructor returns.

=item * purge_on_destroy

If set to a true value, purge will be called before the object goes
out of scope.

=item * clear_on_init

If set to a true value, clear will be called before the constructor returns.

=item * clear_on_destroy

If set to a true value, clear will be called before the object goes
out of scope.

=back

=cut

sub new {
    my ($proto, %params) = @_;
    my $class = ref($proto) || $proto;

    die "Cache::BDB require Berkeley DB version 3 or greater"
	unless $BerkeleyDB::db_version >= 3;
    die "cache_root not specified" unless($params{cache_root});
    die "namespace not specified" unless($params{namespace});

    my $t = time();

    my $home = $params{cache_root};

    unless(-d $home) {
	unless(mkdir($home)) {
	    die "cache_root unavailable: $home $!";
	}
    }

    my $fname = $params{cache_file} || join('.', $params{namespace}, "db");

    my $env = BerkeleyDB::Env->new(
				   -Home => $home,
				   -Flags => 
				   (DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL),
				   -ErrFile => *STDERR,
				   -SetFlags => 
				   $params{env_lock} ? DB_CDB_ALLDB : 0,
				   -Verbose => 1,
				  ) 
      or die "Unable to create env: $! $BerkeleyDB::Error";

    my ($db);

    if(-e $fname) { 
      # cache file(s) exist, connect with Unknown
      $db = BerkeleyDB::Unknown->new(
			  -Env => $env,
			  -Subname => $params{namespace},	     
			  -Filename => $fname,
			  -Pagesize => 8192,
			  )
	or die "Unable to open db: $! $BerkeleyDB::Error";

    }
    else {
      my $type = join('::', 'BerkeleyDB', ($params{type} &&
					   ($params{type} eq 'Btree' ||
					    $params{type} eq 'Hash')) ?
		      $params{type} : DEFAULT_DB_TYPE);

      $db = $type->new(
		       -Env => $env,
		       -Subname => $params{namespace},	     
		       -Filename => $fname,
		       -Flags => DB_CREATE,
		       -Pagesize => 8192,
		      )
	or die "Unable to open db: $! $BerkeleyDB::Error";
    }

    $db->filter_store_value( sub { $_ = Storable::freeze($_) });
    $db->filter_fetch_value( sub { $_ = Storable::thaw($_) });

    my $self = {
		# private stuff
		__env => $env,
		__db => $db,
		__last_purge_time => $t,
		
		# expiry/purge
		default_expires_in => $params{default_expires_in} || 0,
		auto_purge_interval => $params{auto_purge_interval} || 0,
		auto_purge_on_set => $params{auto_purge_on_set} || 0,
		auto_purge_on_get => $params{auto_purge_on_get} || 0,

		purge_on_init => $params{purge_on_init} || 0,
		purge_on_destroy => $params{purge_on_destroy} || 0,

		clear_on_init => $params{clear_on_init} || 0,
		clear_on_destroy => $params{clear_on_destroy} || 0,

		# file/namespace
		namespace => $params{namespace},
		cache_file => $fname,
		cache_root => $params{cache_root},

	       };

    bless $self, $class;

    $self->clear() if $self->{clear_on_init};
    $self->purge() if $self->{purge_on_init};
    $self->{__db}->db_sync();

    return $self;
}

sub DESTROY {
    my $self = shift;

    $self->clear() if $self->{clear_on_destroy};
    $self->purge() if $self->{purge_on_destroy};
#    $self->{__cache}->db_sync();
}

=over 4

=item B<namespace>()

This read only method returns the namespace that the cache object is
currently associated with.

=cut

sub namespace {
    my $self = shift;
    die "namespace is read only" if shift;
    return $self->{namespace};
}

=item B<auto_purge_interval>($seconds)

Set/get the length of time (in seconds) that the cache object will
wait before calling one or both of the B<auto_purge> methodss. If set
to 0, automatic purging is disabled.

=cut

sub auto_purge_interval {
    my ($self, $interval) = @_;

    if(defined($interval)) {
	return undef unless $interval =~ /^\d+$/;
	$self->{auto_purge_interval} = $interval;
    }
    return $self->{auto_purge_interval};
}

=item B<auto_purge_on_set>(1/0)

Enable/disable auto purge when B<set> is called.

=cut

sub auto_purge_on_set {
    my ($self, $v) = @_;
    if(defined($v)) {
	$self->{auto_purge_on_set} = $v;
    }
    return $self->{auto_purge_on_set};
}

=item B<auto_purge_on_get>(1/0)

Enable/disable auto purge when B<get> is called.

=cut

sub auto_purge_on_get {
    my ($self, $v) = @_;
    if(defined($v)) {
	$self->{auto_purge_on_get} = $v;
    }
    return $self->{auto_purge_on_get};
}

=item B<set>($key, $value, [$seconds]) 

Store an item ($value) with the associated $key. Time to live (in
seconds) can be optionally set with a third argument.

=cut

sub set {
    my ($self, $key, $value, $ttl) = @_;

    return undef unless ($key && $value);
    my $rv;
    my $now = time();

    my $interval = $self->{auto_purge_interval};
    if($self->{auto_purge_on_set} && 
       $now > ($self->{__last_purge_time} + $interval)) {
	$self->purge();
	$self->{__last_purge_time} = $now;
    }

    $ttl ||= $self->{default_expires_in};
    my $expires = ($ttl) ? $now + $ttl : 0;
    
    my $data = {__expires => $expires,
		__set_time => $now, 
		__last_access_time => $now,
		__version => $Cache::BDB::VERSION,
		__data => $value};

    $rv = $self->{__db}->db_put($key, $data);

    return $rv;
}

=item B<get>($key)

Locate and return the data associated with $key. Returns undef if the
data doesn't exist. If B<auto_purge_on_get> is enabled, the cache will
be purged before attempting to locate the item.

=cut

sub get {
    my ($self, $key) = @_;

    return undef unless $key;
    my $t = time();

    my $data;
    my $interval = $self->{auto_purge_interval};
    if($self->{auto_purge_on_get} && 
       $t > ($self->{__last_purge_time} + $interval)) {
	$self->purge();
	$self->{__last_purge_time} = $t;
    }

    my $rv = $self->{__db}->db_get($key, $data);
    return undef if $rv == DB_NOTFOUND;

    if($self->__is_expired($data, $t)) {
      $self->remove($key);
      return undef;
    } 
    else {
      # this is way too slow.
      #$self->_update_access_time($key, $data, $t); 

      return $data->{__data};
    }
}

sub _update_access_time {
    my ($self, $key, $data, $t)  = @_;
    
    $t ||= time();
    $data->{__last_access_time} = $t;

    my $rv = $self->{__db}->db_put($key, $data);

    return $rv;
}

=item B<remove>($key)

Removes the cache element specified by $key if it exists.

=cut

sub remove {
    my ($self, $key) = @_;

    my $rv;
    my $v = '';
    $rv = $self->{__db}->db_del($key);
    return $rv;
}

=item B<clear>()

Completely clear out the cache.

=cut

sub clear {
    my $self = shift;
    my $count = 0;
    my $rv;
    $rv = $self->{__db}->truncate($count);
    return $count;
}

=item B<count>

Returns the number of items in the cache.

=cut

sub count {
    my $self = shift;
    my $stats = $self->{__db}->db_stat;

    my $type = $self->{__db}->type;
    return ($type == DB_HASH) ? $stats->{hash_ndata} : $stats->{bt_ndata};

}

=item B<size>

Currently broken. Return the size (in bytes) of all the cached items.
In the future, maybe a callback can be set that calculates the size of the data
however the user wants, and that data stored upon set in the meta data.

=cut 

sub size {
    my $self = shift;

    # no op for now
    return 0;

    my ($k, $v) = ('','');
    my $size = 0;

    my $cursor = $self->{__datadb}->db_cursor();
    while($cursor->c_get($k, $v, DB_NEXT) == 0) {
	$size += length($v->{_data}->{$_}) for (keys %{$v->{__data}});
    }

    $cursor->c_close();
    return $size;
}

=item B<purge>

Purge expired items from the cache.

=cut

sub purge {
    my $self = shift;

    my ($k, $v) = ('','');
    my $t = time();
    my $count = 0;

    my $cursor = $self->{__db}->db_cursor(DB_WRITECURSOR);
    
    while($cursor->c_get($k, $v, DB_NEXT) == 0) {
	if($self->__is_expired($v, $t)) {
	    $cursor->c_del();
	    $self->remove($k);
	    $count++;
	}
    }
    $cursor->c_close();

    return $count;
}

sub __is_expired {
    my ($self, $data, $t) = @_;
    $t ||= time();

    return 1 if($data->{__expires} && $data->{__expires} < $t);
    return 0;
}

=item B<is_expired>($key)

Returns true if the data pointed to by $key is expired based on its
stored expiration time.

=cut

sub is_expired {
    my ($self, $key) = @_;

    my $data;
    my $t = time();
    return undef unless $key;
    my $rv = $self->{__db}->db_get($key, $data);

    return undef unless $data;
    return $self->__is_expired($data, $t);
}

=back

=head1 AUTHOR

Josh Rotenberg, C<< <joshrotenberg at gmail.com> >>

=head1 TODO

* Make data storage scheme configurable (Storable, YAML, Data::Dumper,
  or callback based)

* Split storage between meta and data for faster operations on meta data.

* Add some size/count aware features.

* Solve the perpetually growing db file problem inherent in Berkeley
  DB by allowing atomic mv/unlink/whatever of cachefiles, possibly
  some kind of cache meta options like 'unlink_on_init'.

* Create some examples.

* Fix fork()'ing tests.

=head1 BUGS

Please report any bugs or feature requests to
C<bug-cache-bdb at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Cache-BDB>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Cache::BDB

You can also look for information at:

=over 4

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Cache-BDB>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Cache-BDB>

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Cache-BDB>

=item * Search CPAN

L<http://search.cpan.org/dist/Cache-BDB>

=back

=head1 ACKNOWLEDGEMENTS

Baldur Kristinsson

=head1 COPYRIGHT & LICENSE

Copyright 2006 Josh Rotenberg, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.



1;

__END__
