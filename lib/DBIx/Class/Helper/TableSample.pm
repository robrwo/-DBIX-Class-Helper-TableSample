package DBIx::Class::Helper::TableSample;

# ABSTRACT: Add support for tablesample clauses

use v5.10;

use strict;
use warnings;

use parent 'DBIx::Class::ResultSet';

our $VERSION = 'v0.1.0';

=head1 SYNOPSIS

In a resultset:

  package MyApp::Schema::ResultSet::Wobbles;

  use base qw/DBIx::Class::ResultSet/;

  __PACKAGE__->load_components( qw/
      Helper::TableSample
  /);

Using the resultset:

  my $rs = $schema->resultset('Wobbles')->search_rs(
    undef,
    {
      columns     => [qw/ id name /],
      tablesample => {
        type     => 'system',
        fraction => 0.5,
      },
    }
  );

This generates the SQL

  SELECT me.id FROM artist me TABLESAMPLE SYSTEM (0.5)

=head1 DESCRIPTION

This helper adds rudimentary support for tablesample queries
to L<DBIx::Class> resultsets.

The C<tablesample> key supports the following options as a hash
reference:

=over

=item C<fraction>

This is the percentage or fraction of the table to sample.

Depending on your database, this can be a decimal or must
be an integer.

The value is not checked by this helper, so you can use
database-specific extensions, e.g. C<1000 ROWS> or C<15 PERCENT>.

=item C<type>

By default, there is no sampling type., e.g. you can simply use:

  my $rs = $schema->resultset('Wobbles')->search_rs(
    undef,
    {
      columns     => [qw/ id name /],
      tablesample => 5,
    }
  );

as an equivalent of

  my $rs = $schema->resultset('Wobbles')->search_rs(
    undef,
    {
      columns     => [qw/ id name /],
      tablesample => { fraction => 5 },
    }
  );

to generate

  SELECT me.id FROM artist me TABLESAMPLE (5)

If your database supports or requires a type, you can specify it,
e.g. C<system> or C<bernoulli>.

=item C<repeatable>

If this key is specified, then it will add a REPEATABLE clause,
e.g.

  my $rs = $schema->resultset('Wobbles')->search_rs(
    undef,
    {
      columns     => [qw/ id name /],
      tablesample => {
        fraction   => 5,
        repeatable => 123456,
      },
    }
  );

to generate

  SELECT me.id FROM artist me TABLESAMPLE (5) REPEATABLE (123456)

=back

=head1 CAVEATS

This module is experimental.

Not all databases support table sampling, they may have different
restrictions.  You should consult your database documentation.

=cut

sub _resolved_attrs {
    my $rs    = $_[0];

    $rs->next::method;

    my $attrs = $rs->{_attrs};

    if ( my $conf = delete $attrs->{tablesample} ) {

        my $from = $attrs->{from};

        $rs->throw_exception('tablesample on joins is not supported')
            if (ref $from eq 'ARRAY') && @$from > 1;

        $conf = { fraction => $conf } unless ref $conf;

        $rs->throw_exception('tablesample must be a hashref')
          unless ref $conf eq 'HASH';

        my $sqla = $rs->result_source->storage->sql_maker;

        my $part_sql = " tablesample";

        if (my $type = $conf->{type}) {
            $part_sql .= " $type";
        }

        my $arg = $conf->{fraction};
        $part_sql .= "($arg)";

        if ( defined $conf->{repeatable} ) {
            $part_sql .= sprintf( ' repeatable (%s)', $conf->{repeatable} );
        }

        if (ref $from eq 'ARRAY') {
            my $sql = $sqla->_from_chunk_to_sql($from->[0]) . $sqla->_sqlcase($part_sql);
            $from->[0] = \$sql;
        }
        else {
            my $sql = $sqla->_from_chunk_to_sql($from) . $sqla->_sqlcase($part_sql);
            $attrs->{from} = \$sql;
        }

    }

    return $attrs;
}

=head1 SEE ALSO

L<DBIx::Class>

=head1 append:AUTHOR

The initial development of this module was sponsored by Science Photo
Library L<https://www.sciencephoto.com>.

=cut

1;
