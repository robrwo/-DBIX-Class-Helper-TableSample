package DBIx::Class::Helper::TableSample;

# ABSTRACT: Add support for tablesample clauses

use v5.10;

use strict;
use warnings;

use parent 'DBIx::Class::ResultSet';

our $VERSION = 'v0.1.0';

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

1;
