package Test::TableSample::Role;

use Test::Roo::Role;

use Class::Load qw/ load_class /;
use Ref::Util qw/ is_plain_hashref /;
use SQL::Abstract::Test import => [qw/ is_same_sql_bind /];

has dsn => (
    is      => 'lazy',
    default => 'dbi:SQLite::memory:',
);

has schema_class => (
    is      => 'ro',
    default => 'Test::Schema',
);

has schema => (
    is      => 'lazy',
    builder => sub {
        my ($self) = @_;
        my $class = $self->schema_class;
        load_class($class);
        return $class->deploy_or_connect( $self->dsn );
    },
);

has table_class => (
    is       => 'ro',
    required => 1,
);

has where => (
    is      => 'ro',
    default => sub { return undef },
);

has attr => (
    is       => 'ro',
    required => 1,
);

has resultset => (
    is      => 'lazy',
    builder => sub {
        my ($self) = @_;
        return $self->schema->resultset( $self->table_class )
          ->search_rs( $self->where, $self->attr );
    },
    handles => [qw/ current_source_alias /],
);

has sql => (
    is       => 'ro',
    required => 1,
);

has bind => (
    is => 'ro',
    default => sub { [] },
);

sub _build_description {
    my ($self) = @_;
    return $self->sql;
}

test 'build query' => sub {
    my ($self) = @_;

    ok my $rs = $self->resultset, 'build resultset';

    my $alias = $self->current_source_alias;
    my $sql = $self->sql =~ s/\bme\b/$alias/gr;

    is_same_sql_bind(
        $rs->as_query,
        "(${sql})",
        $self->bind,
        'expected sql and bind'
    );

};

test 'tablesample method' => sub {
    my ($self) = @_;

    my %attr = %{ $self->attr };
    ok my $opts = delete $attr{tablesample}, 'tablesample options';

    unless ( is_plain_hashref($opts) ) {
        $opts = { fraction => $opts };
    }

    my $frac = delete $opts->{fraction};

    {
        my $rs = $self->schema->resultset( $self->table_class )->search_rs( $self->where, \%attr )->tablesample( $frac, $opts );

        my $alias = $self->current_source_alias;
        my $sql   = $self->sql =~ s/\bme\b/$alias/gr;

        is_same_sql_bind( $rs->as_query, "(${sql})", $self->bind, 'expected sql and bind' );
    }

  SKIP: {
        my $method = delete $opts->{method};
        skip "no method" unless $method;
        skip "other options" if scalar( keys %$opts );

        my $rs = $self->schema->resultset( $self->table_class )->search_rs( $self->where, \%attr )->tablesample( $frac, $method );

        my $alias = $self->current_source_alias;
        my $sql   = $self->sql =~ s/\bme\b/$alias/gr;

        is_same_sql_bind( $rs->as_query, "(${sql})", $self->bind, 'expected sql and bind (method name instead of hashref)' );

    }

};

1;
